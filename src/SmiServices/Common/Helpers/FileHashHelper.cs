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
    public static string ComputeSha256Hash(IFileSystem fileSystem, string filePath)
    {
        using var stream = fileSystem.File.OpenRead(filePath);
        using var sha256 = SHA256.Create();
        byte[] hashBytes = sha256.ComputeHash(stream);
        return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
    }
}
