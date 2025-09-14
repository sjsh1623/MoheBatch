
package com.mohe.spring.config;

import com.mohe.spring.service.LlmService;
import com.mohe.spring.service.OllamaService;
import com.mohe.spring.service.OpenAiService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LlmServiceFactory {

    @Bean
    public LlmService llmService(LlmProperties llmProperties) {
        if (llmProperties.isOpenAiActive()) {
            return new OpenAiService(llmProperties);
        } else {
            return new OllamaService(llmProperties);
        }
    }
}
