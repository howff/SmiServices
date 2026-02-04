using System;
using System.IO.Abstractions;
using System.Security.Cryptography;

namespace SmiServices.Common.Helpers;

/// <summary>
/// Utility class for computing file hashes
/// </summary>
public static class FileHashHelper
{
    /// <summary>
    /// Computes the SHA256 hash of a file
    /// </summary>
    /// <param name="fileSystem">The file system abstraction to use</param>
    /// <param name="filePath">The path to the file to hash</param>
    /// <returns>The SHA256 hash as a lowercase hexadecimal string</returns>
    /// <exception cref="ArgumentNullException">Thrown when fileSystem or filePath is null</exception>
    /// <exception cref="System.IO.FileNotFoundException">Thrown when the file does not exist</exception>
    public static string ComputeSha256Hash(IFileSystem fileSystem, string filePath)
    {
        if (fileSystem == null)
            throw new ArgumentNullException(nameof(fileSystem));
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentNullException(nameof(filePath));
        if (!fileSystem.File.Exists(filePath))
            throw new System.IO.FileNotFoundException($"File not found: {filePath}", filePath);

        using var stream = fileSystem.File.OpenRead(filePath);
        using var sha256 = SHA256.Create();
        byte[] hashBytes = sha256.ComputeHash(stream);
        return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
    }
}
