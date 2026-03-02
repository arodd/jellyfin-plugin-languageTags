using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Extensions;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Model.Entities;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.LanguageTags.Services;

/// <summary>
/// Resolves spoken-language and country-of-origin data from IMDb metadata.
/// </summary>
public sealed class ImdbSpokenLanguageService : IDisposable
{
    /// <summary>
    /// Prefix for IMDb spoken-language tags.
    /// </summary>
    public const string ImdbLanguageTagPrefix = "languageimdb_";

    /// <summary>
    /// Prefix for IMDb origin-country tags.
    /// </summary>
    public const string OriginCountryTagPrefix = "origincountry_";

    /// <summary>
    /// Tag added when IMDb primary spoken language is non-English.
    /// </summary>
    public const string ForeignTag = "foreign";

    private static readonly HttpClient HttpClient = CreateHttpClient();
    private readonly ILogger<ImdbSpokenLanguageService> _logger;
    private readonly SemaphoreSlim _requestLimiter = new(4, 4);
    private readonly ConcurrentDictionary<string, ImdbSpokenLanguageResult?> _cache =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Initializes a new instance of the <see cref="ImdbSpokenLanguageService"/> class.
    /// </summary>
    /// <param name="logger">Instance of the logger.</param>
    public ImdbSpokenLanguageService(ILogger<ImdbSpokenLanguageService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Gets IMDb spoken-language and country-of-origin metadata for a media item.
    /// </summary>
    /// <param name="item">The media item.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>IMDb metadata result when available; otherwise null.</returns>
    public async Task<ImdbSpokenLanguageResult?> TryGetImdbMetadataAsync(BaseItem item, CancellationToken cancellationToken)
    {
        if (item is null)
        {
            return null;
        }

        var imdbId = item.GetProviderId(MetadataProvider.Imdb);
        if (!IsValidImdbId(imdbId))
        {
            return null;
        }

        var normalizedImdbId = imdbId!;

        if (_cache.TryGetValue(normalizedImdbId, out var cachedResult))
        {
            return cachedResult;
        }

        ImdbSpokenLanguageResult? resolvedResult = null;
        var hasLock = false;

        try
        {
            await _requestLimiter.WaitAsync(cancellationToken).ConfigureAwait(false);
            hasLock = true;
            resolvedResult = await FetchImdbMetadataAsync(normalizedImdbId, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Unable to resolve IMDb metadata for {ImdbId}", normalizedImdbId);
        }
        finally
        {
            if (hasLock)
            {
                _requestLimiter.Release();
            }
        }

        _cache.TryAdd(normalizedImdbId, resolvedResult);
        return resolvedResult;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _requestLimiter.Dispose();
    }

    private static HttpClient CreateHttpClient()
    {
        var client = new HttpClient
        {
            BaseAddress = new Uri("https://caching.graphql.imdb.com/"),
            Timeout = TimeSpan.FromSeconds(15)
        };

        client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Jellyfin.LanguageTags.Plugin");
        return client;
    }

    private static bool IsValidImdbId(string? imdbId)
    {
        return !string.IsNullOrWhiteSpace(imdbId)
            && imdbId.StartsWith("tt", StringComparison.OrdinalIgnoreCase)
            && imdbId.Length >= 3;
    }

    private async Task<ImdbSpokenLanguageResult?> FetchImdbMetadataAsync(string imdbId, CancellationToken cancellationToken)
    {
        var payload = new
        {
            query = "query($id: ID!) { title(id: $id) { spokenLanguages { spokenLanguages { id text } } countriesOfOrigin { countries { id text } } } }",
            variables = new { id = imdbId }
        };

        using var response = await HttpClient
            .PostAsJsonAsync(string.Empty, payload, cancellationToken)
            .ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogDebug(
                "IMDb GraphQL request failed for {ImdbId} with HTTP {StatusCode}",
                imdbId,
                (int)response.StatusCode);
            return null;
        }

        using var body = await response.Content
            .ReadAsStreamAsync(cancellationToken)
            .ConfigureAwait(false);

        using var document = await JsonDocument.ParseAsync(body, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!TryGetImdbLanguageAndCountryData(
                document,
                out var primaryLanguageId,
                out var primaryLanguageName,
                out var spokenLanguages,
                out var originCountries))
        {
            return null;
        }

        if (spokenLanguages.Count == 0 && originCountries.Count == 0)
        {
            return null;
        }

        var isEnglish = IsEnglishLanguage(primaryLanguageId, primaryLanguageName ?? string.Empty);
        return new ImdbSpokenLanguageResult(primaryLanguageName, isEnglish, spokenLanguages, originCountries);
    }

    private static bool TryGetImdbLanguageAndCountryData(
        JsonDocument document,
        out string primaryLanguageId,
        out string? primaryLanguageName,
        out IReadOnlyList<string> spokenLanguages,
        out IReadOnlyList<string> originCountries)
    {
        primaryLanguageId = string.Empty;
        primaryLanguageName = null;
        spokenLanguages = Array.Empty<string>();
        originCountries = Array.Empty<string>();

        var root = document.RootElement;
        if (!root.TryGetProperty("data", out var dataElement))
        {
            return false;
        }

        if (!dataElement.TryGetProperty("title", out var titleElement))
        {
            return false;
        }

        var spoken = ParseSpokenLanguages(titleElement, out primaryLanguageId, out primaryLanguageName);
        var countries = ParseOriginCountries(titleElement);
        spokenLanguages = spoken;
        originCountries = countries;

        return spoken.Count > 0 || countries.Count > 0;
    }

    private static string NormalizeLanguageName(string languageName)
    {
        var normalized = languageName.Trim();
        var qualifierIndex = normalized.IndexOf(" (", StringComparison.Ordinal);
        if (qualifierIndex > 0)
        {
            normalized = normalized[..qualifierIndex].Trim();
        }

        return normalized;
    }

    private static string NormalizeCountryName(string countryName)
    {
        return countryName.Trim();
    }

    private static List<string> ParseSpokenLanguages(
        JsonElement titleElement,
        out string primaryLanguageId,
        out string? primaryLanguageName)
    {
        primaryLanguageId = string.Empty;
        primaryLanguageName = null;
        var results = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (!titleElement.TryGetProperty("spokenLanguages", out var spokenLanguagesElement))
        {
            return results;
        }

        if (!spokenLanguagesElement.TryGetProperty("spokenLanguages", out var entriesElement)
            || entriesElement.ValueKind != JsonValueKind.Array
            || entriesElement.GetArrayLength() == 0)
        {
            return results;
        }

        var primaryCaptured = false;
        foreach (var entry in entriesElement.EnumerateArray())
        {
            var textValue = entry.TryGetProperty("text", out var textElement)
                ? textElement.GetString()
                : null;
            if (string.IsNullOrWhiteSpace(textValue))
            {
                continue;
            }

            var normalizedLanguageName = NormalizeLanguageName(textValue);
            if (string.IsNullOrWhiteSpace(normalizedLanguageName))
            {
                continue;
            }

            if (!primaryCaptured)
            {
                primaryCaptured = true;
                primaryLanguageName = normalizedLanguageName;
                primaryLanguageId = entry.TryGetProperty("id", out var idElement)
                    ? idElement.GetString() ?? string.Empty
                    : string.Empty;
            }

            if (seen.Add(normalizedLanguageName))
            {
                results.Add(normalizedLanguageName);
            }
        }

        return results;
    }

    private static List<string> ParseOriginCountries(JsonElement titleElement)
    {
        var results = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (!titleElement.TryGetProperty("countriesOfOrigin", out var countriesOfOriginElement))
        {
            return results;
        }

        if (!countriesOfOriginElement.TryGetProperty("countries", out var countriesElement)
            || countriesElement.ValueKind != JsonValueKind.Array
            || countriesElement.GetArrayLength() == 0)
        {
            return results;
        }

        foreach (var countryEntry in countriesElement.EnumerateArray())
        {
            var textValue = countryEntry.TryGetProperty("text", out var textElement)
                ? textElement.GetString()
                : null;
            if (string.IsNullOrWhiteSpace(textValue))
            {
                continue;
            }

            var normalizedCountryName = NormalizeCountryName(textValue);
            if (string.IsNullOrWhiteSpace(normalizedCountryName))
            {
                continue;
            }

            if (seen.Add(normalizedCountryName))
            {
                results.Add(normalizedCountryName);
            }
        }

        return results;
    }

    private static bool IsEnglishLanguage(string languageId, string languageName)
    {
        if (!string.IsNullOrWhiteSpace(languageId)
            && languageId.StartsWith("en", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return languageName.Equals("English", StringComparison.OrdinalIgnoreCase)
            || languageName.StartsWith("English ", StringComparison.OrdinalIgnoreCase);
    }
}
