# XA Modality External Anonymization

This document describes how XA (X-ray Angiographic) modality DICOM files are anonymized using an external program.

## Overview

The DicomAnonymiser microservice now supports modality-specific anonymization routing. XA modality files can be processed by an external anonymization tool, similar to how SR (Structured Reports) modality is handled, but with key architectural differences.

## How SR Modality Is Anonymized

**SR (Structured Reports)** modality uses an optional external tool that is integrated with the CTP (Clinical Trials Processor) infrastructure:

- Configured via `SRAnonymiserToolPath` in `DicomAnonymiserOptions`
- Passed as `--sr-anon-tool` argument to the CTP Java process
- The tool is called by CTP itself, not directly by SmiServices
- Still processes through the CTP daemon infrastructure

## How XA Modality Is Anonymized

**XA (X-ray Angiographic)** modality uses a dedicated external anonymizer that bypasses CTP:

- Configured via `XaAnonymiserToolPath` in `DicomAnonymiserOptions`
- Called directly by SmiServices via `XaExternalAnonymiser` class
- Completely independent of CTP infrastructure
- Provides better isolation and control over the anonymization process

## Architecture

### Components

1. **IDicomAnonymiser Interface** - Defines the contract for all anonymizers
   ```csharp
   ExtractedFileStatus Anonymise(
       IFileInfo sourceFile, 
       IFileInfo destFile, 
       string modality, 
       out string? anonymiserStatusMessage
   );
   ```

2. **XaExternalAnonymiser** - New anonymiser for XA modality
   - Executes external program with source and destination file paths
   - Handles process timeout (60 seconds default)
   - Validates exit code and output file existence
   - Returns `ExtractedFileStatus.Anonymised` on success
   - Returns `ExtractedFileStatus.ErrorWontRetry` on failure

3. **DefaultAnonymiser** - Routes different modalities to appropriate anonymizers
   - Checks modality string and routes "XA" to `XaExternalAnonymiser` when configured
   - Falls back to `SmiCtpAnonymiser` for all other modalities
   - Only initializes XA anonymiser if `XaAnonymiserToolPath` is configured

4. **SmiCtpAnonymiser** - Existing CTP-based anonymiser
   - Handles all other modalities (and XA if XA tool not configured)
   - Maintains Java CTP daemon process

### Routing Logic

```
ExtractFileMessage → DicomAnonymiserConsumer → DefaultAnonymiser
                                                      ↓
                                    ┌─────────────────┴─────────────────┐
                                    ↓                                   ↓
                          modality == "XA" &&                  All other modalities
                          XaToolConfigured?                    (or XA without tool)
                                    ↓                                   ↓
                          XaExternalAnonymiser              SmiCtpAnonymiser
                                    ↓                                   ↓
                          External Program                    CTP Java Process
```

## Configuration

Add the following to your `DicomAnonymiserOptions` configuration:

```yaml
DicomAnonymiserOptions:
  AnonymiserType: DefaultAnonymiser  # Required to enable routing
  XaAnonymiserToolPath: /path/to/xa-anonymiser-tool
  # ... other CTP configuration still required for non-XA modalities
  CtpAnonCliJar: /path/to/ctp-anon-cli.jar
  CtpAllowlistScript: /path/to/ctp-allowlist.script
```

### Configuration Options

- **XaAnonymiserToolPath** (optional): Path to the external XA anonymization program
  - If not set or empty, XA files will be processed by CTP like other modalities
  - Must be an executable file (e.g., shell script, Python script, compiled binary)
  - Path is validated at initialization - file must exist

## External Program Interface

The external XA anonymization program must:

1. **Accept two command-line arguments:**
   - Argument 1: Source DICOM file path (input)
   - Argument 2: Destination DICOM file path (output)

2. **Return exit code 0 on success**, non-zero on failure

3. **Create the output file** at the destination path

4. **Complete within 60 seconds** (configurable in code)

### Example External Program (Bash)

```bash
#!/bin/bash
# xa-anonymizer.sh

SOURCE_FILE="$1"
DEST_FILE="$2"

# Perform anonymization (example uses dcmtk's dcmodify)
dcmodify --no-backup \
  --erase "(0010,0010)" \  # Patient Name
  --erase "(0010,0020)" \  # Patient ID
  --insert "(0010,0010)=ANONYMOUS" \
  "$SOURCE_FILE" "$DEST_FILE"

exit $?
```

### Example External Program (Python)

```python
#!/usr/bin/env python3
import sys
import pydicom

def anonymize_xa(source_path, dest_path):
    """Anonymize XA DICOM file."""
    ds = pydicom.dcmread(source_path)
    
    # Remove patient identifiable information
    ds.PatientName = "ANONYMOUS"
    ds.PatientID = ""
    ds.PatientBirthDate = ""
    
    # Save anonymized file
    ds.save_as(dest_path)
    return 0

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: xa-anonymizer.py <source> <dest>", file=sys.stderr)
        sys.exit(1)
    
    try:
        result = anonymize_xa(sys.argv[1], sys.argv[2])
        sys.exit(result)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
```

## Error Handling

The `XaExternalAnonymiser` handles the following error conditions:

1. **Tool not found** - Throws `ArgumentException` during initialization
2. **Process timeout** - Returns `ErrorWontRetry` with timeout message
3. **Non-zero exit code** - Returns `ErrorWontRetry` with stderr output
4. **Missing output file** - Returns `ErrorWontRetry` even if exit code was 0
5. **Process exception** - Returns `ErrorWontRetry` with exception details

All errors are logged with appropriate severity levels.

## Testing

Unit tests are provided for:

- `XaExternalAnonymiser` 
  - Happy path (successful anonymization)
  - Tool not found
  - Tool path not set
  - Tool fails with exit code
  
- `DefaultAnonymiser`
  - Initialization with XA tool configured
  - Initialization without XA tool configured

Note: Tests requiring bash scripts are skipped on Windows platforms.

## Key Differences: SR vs XA

| Aspect | SR Modality | XA Modality |
|--------|-------------|-------------|
| **Integration** | Via CTP Java process | Direct process execution |
| **Configuration** | `SRAnonymiserToolPath` | `XaAnonymiserToolPath` |
| **Process Control** | CTP manages the tool | SmiServices manages the tool |
| **Isolation** | Shared CTP daemon | Independent process per file |
| **Timeout** | Inherited from CTP | Configurable (60s default) |
| **Error Handling** | Via CTP output | Direct process monitoring |

## Performance Considerations

- **Process Creation Overhead**: Each XA file spawns a new process
- **Timeout**: Set to 60 seconds - adjust in code if needed for large files
- **Memory**: Each process runs independently, avoiding shared state issues
- **Concurrency**: Multiple XA anonymizations can run in parallel (via consumer scaling)

## Future Enhancements

Potential improvements for consideration:

1. Make timeout configurable via `DicomAnonymiserOptions`
2. Add support for other modalities with similar external tool pattern
3. Pool processes to reduce creation overhead
4. Add metrics/telemetry for external tool performance
5. Support for tool-specific configuration passing (e.g., anonymization level)
