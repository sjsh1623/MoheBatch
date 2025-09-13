# Enhanced Image Generation Batch Job

This document describes the enhanced batch job for place image generation that integrates Google Places Photos API with Gemini CLI fallback.

## Overview

The `placeImageGenerationJob` is a Spring Batch job that processes places in the database to ensure every place has a valid image. The job uses a two-tier approach:

1. **Primary**: Google Places Photos API - Downloads real photos from Google Places
2. **Fallback**: Gemini CLI - Generates AI images when no photos are available

## Processing Logic

### Target Places

The batch processes:
- All places with `image_url IS NULL OR image_url = ''`
- Places where `image_url` points to missing files on disk  
- All existing places (when `--force` flag is used)

### Image Retrieval Strategy

For each place:

1. **Google Photos API First**:
   - Extract photo references from `googleData` field
   - Try New API: `GET https://places.googleapis.com/v1/{name=places/*/photos/*}/media`
   - Fallback to Legacy API: `GET https://maps.googleapis.com/maps/api/place/photo`
   - Download image as JPG format

2. **Gemini CLI Fallback**:
   - Generate prompt using Ollama from place name, description, keywords
   - Execute configurable Gemini CLI command
   - Generate image with specified dimensions

### File Storage

**Directory Structure**:
```
{PUBLISH_ROOT_DIR}/images/places/<yyyy>/<mm>/
```

**File Naming**:
```
<placeId>__<sanitizedPlaceName>.jpg
```

**Database Storage**:
- Store only relative path: `/images/places/<yyyy>/<mm>/<file>.jpg`
- Accessible via: `http://localhost:1000` + `places.image_url`

## Configuration

### Required Environment Variables

**Google Places API**:
```bash
GOOGLE_PLACES_API_KEY=your_google_places_api_key
```

**Gemini CLI**:
```bash
GEMINI_CLI_CMD=gemini
GEMINI_CLI_ARGS_TPL='image:generate -p "{PROMPT}" -o "{OUTPUT}"'
GEMINI_IMAGE_SIZE=1024x1024
```

**File Publishing**:
```bash
PUBLISH_ROOT_DIR=/path/to/static/files
PUBLISH_BASE_PATH=/images/places
```

**Retry/Backoff Settings**:
```bash
BACKOFF_INITIAL_MS=30000
BACKOFF_MAX_MS=600000
```

**Ollama Configuration**:
```bash
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama3
```

### Application Configuration

The job uses existing configuration in `application.yml`:

```yaml
app:
  google:
    api-key: ${GOOGLE_PLACES_API_KEY:}
    
  batch:
    image-generation:
      cli:
        command: ${GEMINI_CLI_CMD:gemini}
        args-template: ${GEMINI_CLI_ARGS_TPL:image:generate -p "{PROMPT}" -o "{OUTPUT}"}
        image-size: ${GEMINI_IMAGE_SIZE:1024x1024}
      publish:
        root-dir: ${PUBLISH_ROOT_DIR:/tmp/static}
        base-path: ${PUBLISH_BASE_PATH:/images/places}
      retry:
        max-attempts: 5
        initial-backoff-ms: ${BACKOFF_INITIAL_MS:30000}
        max-backoff-ms: ${BACKOFF_MAX_MS:600000}
      concurrency: ${MAX_CONCURRENCY:4}
```

## Usage

### Basic Usage

```bash
# Process all places without images
java -jar mohebatch.jar --job=placeImageGenerationJob

# Process with limit
java -jar mohebatch.jar --job=placeImageGenerationJob --limit=100

# Process places updated since date
java -jar mohebatch.jar --job=placeImageGenerationJob --since=2025-01-01
```

### Command Line Options

- `--limit=N`: Limit processing to N places
- `--since=YYYY-MM-DD`: Only process places updated since this date
- `--force=true`: Regenerate images for all places (ignore existing)
- `--dryRun=true`: Log actions without making changes

### Examples

```bash
# Dry run to see what would be processed
java -jar mohebatch.jar --job=placeImageGenerationJob --dryRun=true --limit=10

# Force regenerate first 50 places
java -jar mohebatch.jar --job=placeImageGenerationJob --force=true --limit=50

# Process recent places only
java -jar mohebatch.jar --job=placeImageGenerationJob --since=2025-09-01
```

## Error Handling

### Google API Quota Management

- **429 (Quota Exceeded)**: Automatic retry with exponential backoff
- **403 (Invalid API Key)**: Skip processing, log error
- **404 (Photo Not Found)**: Fall back to Gemini CLI
- **Other errors**: Log and fall back to Gemini CLI

### Gemini CLI Quota Management

- **429 errors in CLI output**: Retry with exponential backoff
- **Other CLI failures**: Skip place with error log
- **Timeout**: Skip place after 120 seconds

### Batch Fault Tolerance

- **Retry**: QuotaExceededException retried 5 times
- **Skip**: General exceptions skipped (up to 100)
- **Never Skip**: Quota errors always retried, never skipped

## Logging

### Structured JSON Logs

Each processing stage logs JSON with:

```json
{
  "place_id": 123,
  "stage": "photo_api|prompt|generate|write|db_update|skip|fail",
  "status": "started|success|failed|no_photo_available|insufficient_input",
  "elapsed_ms": 1234,
  "error": "error message if failed"
}
```

### Log Levels

- **INFO**: Processing progress and successful operations
- **WARN**: API errors, quota issues, fallback usage
- **DEBUG**: Detailed processing steps, photo extraction
- **ERROR**: Critical failures requiring attention

## Monitoring

### Key Metrics to Monitor

1. **Processing Rate**: Places processed per minute
2. **Google API Success Rate**: Photos retrieved vs attempts  
3. **Quota Usage**: Google API calls vs daily limits
4. **Fallback Rate**: Gemini CLI usage percentage
5. **Error Rate**: Failed places vs total processed

### Health Checks

- Monitor disk space in `PUBLISH_ROOT_DIR`
- Check Google API key validity
- Verify Gemini CLI tool availability
- Monitor Ollama service health

## Troubleshooting

### Common Issues

**No images processed**:
- Check `GOOGLE_PLACES_API_KEY` validity
- Verify places have `googleData` with photo references
- Ensure `PUBLISH_ROOT_DIR` exists and is writable

**High error rate**:
- Check API quotas not exceeded
- Verify network connectivity to APIs
- Check disk space availability

**Gemini CLI failures**:
- Test CLI command manually
- Verify `GEMINI_CLI_CMD` in PATH
- Check CLI tool authentication

**Memory issues**:
- Reduce concurrency settings
- Increase JVM heap size
- Lower chunk size in batch configuration

### Debug Commands

```bash
# Check Google API connectivity
curl "https://places.googleapis.com/v1/places/search/text" \
  -H "X-Goog-Api-Key: ${GOOGLE_PLACES_API_KEY}"

# Test Gemini CLI manually  
gemini image:generate -p "A cozy coffee shop" -o "/tmp/test.png"

# Verify Ollama service
curl http://localhost:11434/api/generate \
  -d '{"model":"llama3","prompt":"test","stream":false}'
```

## Performance Optimization

### Recommended Settings

**For high throughput**:
```bash
MAX_CONCURRENCY=8
BATCH_CHUNK_SIZE=20
GOOGLE_TIMEOUT=10
```

**For quota conservation**:
```bash
MAX_CONCURRENCY=2
BATCH_CHUNK_SIZE=5
GOOGLE_TIMEOUT=30
```

### Best Practices

1. **Start with dry run** to estimate scope
2. **Use `--limit`** for initial testing
3. **Monitor API quotas** during processing
4. **Schedule during low-traffic hours** 
5. **Backup existing images** before `--force` operations

## File Management

### Cleanup Operations

Old or invalid image files should be periodically cleaned:

```bash
# Find orphaned files (no matching DB records)
# Find files older than 6 months with no recent access
```

### Static File Serving

Ensure static file server is configured:
- Port 1000 serves files from `PUBLISH_ROOT_DIR`
- MIME type mapping for `.jpg` files
- Proper cache headers for performance

## Schema Compatibility

**No schema changes required**:
- Uses existing `places.image_url` column
- Compatible with current database structure
- Works with existing place data

This enhanced batch job ensures comprehensive image coverage for all places while maintaining efficiency and reliability through proper error handling and quota management.