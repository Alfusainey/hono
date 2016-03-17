package org.eclipse.hono.adapter.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

public class JacksonPayloadEncoder implements PayloadEncoder {

    private final ObjectMapper objectMapper;

    public JacksonPayloadEncoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public JacksonPayloadEncoder() {
        this(new ObjectMapper().setSerializationInclusion(NON_NULL));
    }

    @Override
    public byte[] encode(Object payload) {
        try {
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("payload", payload);
            return objectMapper.writeValueAsBytes(envelope);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object decode(byte[] payload) {
        try {
            return objectMapper.readValue(payload, Map.class).get("payload");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
