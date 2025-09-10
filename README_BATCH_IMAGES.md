# Batch Job: Place Image Generation

This document outlines how to run the batch job responsible for generating and publishing images for places using a command-line interface (CLI) for image generation.

## Overview

The `placeImageGenerationJob` is a Spring Batch job that iterates through the `places` table. For each place that does not have an image, it performs the following steps:

1.  **Generates a Prompt**: It uses an internal Ollama integration to generate a high-quality, descriptive prompt suitable for a text-to-image model.
2.  **Generates an Image**: It executes a configurable command-line tool (e.g., Gemini CLI) to generate an image based on the prompt.
3.  **Publishes the Image**: It saves the generated image to a designated static file directory.
4.  **Updates the Database**: It updates the `places.image_url` column with the public path to the newly generated image.

The job is designed to be idempotent and restart-safe.

## Job Parameters

The job accepts the following optional parameters:

-   `--force`: (Boolean, default: `false`) If set to `true`, the job will regenerate images for all targeted places, even if an image file already exists.
-   `--limit`: (Long, default: `null`) If set, the job will only process the specified number of places.
-   `--dry-run`: (Boolean, default: `false`) If set to `true`, the job will execute all steps except for writing files to disk and updating the database. It will log the actions it would have taken.
-   `--since`: (String, format: `YYYY-MM-DD`, default: `null`) If provided, the job will only consider places updated since this date.

## Environment Variables

The job requires the following environment variables to be configured:

### Ollama Configuration
-   `OLLAMA_BASE_URL`: The base URL for the Ollama API (e.g., `http://localhost:11434`).
-   `OLLAMA_MODEL`: The name of the Ollama model to use for generating prompts (e.g., `llama3`).

### Image Generation CLI Configuration
-   `GEMINI_CLI_CMD`: The command to execute the image generation CLI (e.g., `gemini`).
-   `GEMINI_CLI_ARGS_TPL`: The argument template for the CLI command. It must contain `{PROMPT}` and `{OUTPUT}` placeholders.
    -   Example: `image:generate -p "{PROMPT}" -o "{OUTPUT}"`
-   `GEMINI_IMAGE_SIZE`: The desired image size (e.g., `1024x1024`).

### File Publishing Configuration
-   `PUBLISH_ROOT_DIR`: The absolute local directory where the static server serves files from.
-   `PUBLISH_BASE_PATH`: The public base path for the image URL (e.g., `/images/places`).

### Concurrency and Retry Configuration
-   `MAX_CONCURRENCY`: The maximum number of parallel threads for the batch step (e.g., `4`).
-   `BACKOFF_INITIAL_MS`: The initial backoff delay in milliseconds for retrying on quota errors (e.g., `30000`).
-   `BACKOFF_MAX_MS`: The maximum backoff delay in milliseconds (e.g., `600000`).

## Known Failure Modes

-   **CLI Quota Errors**: If the image generation CLI returns a quota-related error (e.g., HTTP 429), the job will log the error and retry the item with an exponential backoff policy.
-   **Insufficient Data**: If a place has insufficient data (e.g., no description and no tags) to generate a meaningful prompt, it will be skipped.
-   **Configuration Errors**: The job will fail to start if required environment variables (like `PUBLISH_ROOT_DIR` or `GEMINI_CLI_CMD`) are not set.
