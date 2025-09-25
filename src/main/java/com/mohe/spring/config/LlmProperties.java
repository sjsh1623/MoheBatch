
package com.mohe.spring.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "llm")
public class LlmProperties {

    private boolean isOpenAiActive = false;
    private final OpenAiProperties openai = new OpenAiProperties();
    private final OllamaProperties ollama = new OllamaProperties();

    public boolean isOpenAiActive() {
        return isOpenAiActive;
    }

    public void setOpenAiActive(boolean openAiActive) {
        isOpenAiActive = openAiActive;
    }

    public OpenAiProperties getOpenai() {
        return openai;
    }

    public OllamaProperties getOllama() {
        return ollama;
    }

    public static class OpenAiProperties {
        private String apiKey = "";
        private String model = "gpt-3.5-turbo";

        public String getApiKey() {
            return apiKey;
        }

        public void setApiKey(String apiKey) {
            this.apiKey = apiKey;
        }

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }
    }

    public static class OllamaProperties {
        private String baseUrl = "http://localhost:11434";
        private String model = "llama3";

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }
    }
}
