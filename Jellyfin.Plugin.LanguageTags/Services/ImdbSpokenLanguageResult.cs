namespace Jellyfin.Plugin.LanguageTags.Services;

/// <summary>
/// Primary IMDb spoken language details.
/// </summary>
/// <param name="LanguageName">Primary spoken language name.</param>
/// <param name="IsEnglish">Indicates whether primary spoken language is English.</param>
public sealed record ImdbSpokenLanguageResult(string LanguageName, bool IsEnglish);
