using System;
using System.Collections.Concurrent;
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
/// Resolves primary spoken language from IMDb metadata.
/// </summary>
public sealed class ImdbSpokenLanguageService : IDisposable
{
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
    /// Gets IMDb primary spoken language for a media item.
    /// </summary>
    /// <param name="item">The media item.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Primary spoken language result when available; otherwise null.</returns>
    public async Task<ImdbSpokenLanguageResult?> TryGetPrimarySpokenLanguageAsync(BaseItem item, CancellationToken cancellationToken)
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
            resolvedResult = await FetchPrimarySpokenLanguageAsync(normalizedImdbId, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Unable to resolve IMDb spoken language for {ImdbId}", normalizedImdbId);
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

    private async Task<ImdbSpokenLanguageResult?> FetchPrimarySpokenLanguageAsync(string imdbId, CancellationToken cancellationToken)
    {
        var payload = new
        {
            query = "query($id: ID!) { title(id: $id) { spokenLanguages { spokenLanguages { id text } } } }",
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
        if (!TryGetPrimarySpokenLanguage(document, out var languageId, out var languageName))
        {
            return null;
        }

        var normalizedName = NormalizeLanguageName(languageName);
        if (string.IsNullOrWhiteSpace(normalizedName))
        {
            return null;
        }

        var isEnglish = IsEnglishLanguage(languageId, normalizedName);
        return new ImdbSpokenLanguageResult(normalizedName, isEnglish);
    }

    private static bool TryGetPrimarySpokenLanguage(JsonDocument document, out string languageId, out string languageName)
    {
        languageId = string.Empty;
        languageName = string.Empty;

        var root = document.RootElement;
        if (!root.TryGetProperty("data", out var dataElement))
        {
            return false;
        }

        if (!dataElement.TryGetProperty("title", out var titleElement))
        {
            return false;
        }

        if (!titleElement.TryGetProperty("spokenLanguages", out var spokenLanguagesElement))
        {
            return false;
        }

        if (!spokenLanguagesElement.TryGetProperty("spokenLanguages", out var entriesElement)
            || entriesElement.ValueKind != JsonValueKind.Array
            || entriesElement.GetArrayLength() == 0)
        {
            return false;
        }

        var firstEntry = entriesElement[0];
        if (!firstEntry.TryGetProperty("text", out var textElement))
        {
            return false;
        }

        languageName = textElement.GetString() ?? string.Empty;

        if (firstEntry.TryGetProperty("id", out var idElement))
        {
            languageId = idElement.GetString() ?? string.Empty;
        }

        return !string.IsNullOrWhiteSpace(languageName);
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
