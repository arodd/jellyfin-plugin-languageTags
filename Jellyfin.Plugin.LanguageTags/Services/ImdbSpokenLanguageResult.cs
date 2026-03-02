using System.Collections.Generic;

namespace Jellyfin.Plugin.LanguageTags.Services;

/// <summary>
/// IMDb spoken language and country-of-origin details.
/// </summary>
/// <param name="PrimarySpokenLanguage">Primary spoken language (spokenLanguages[0]), when available.</param>
/// <param name="IsPrimarySpokenLanguageEnglish">Indicates whether primary spoken language is English.</param>
/// <param name="SpokenLanguages">All spoken languages returned by IMDb for the title.</param>
/// <param name="OriginCountries">All countries of origin returned by IMDb for the title.</param>
public sealed record ImdbSpokenLanguageResult(
    string? PrimarySpokenLanguage,
    bool IsPrimarySpokenLanguageEnglish,
    IReadOnlyList<string> SpokenLanguages,
    IReadOnlyList<string> OriginCountries);
