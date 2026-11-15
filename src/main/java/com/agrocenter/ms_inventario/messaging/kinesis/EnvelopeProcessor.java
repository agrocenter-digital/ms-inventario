package com.agrocenter.ms_inventario.messaging.kinesis;

import com.agrocenter.ms_inventario.messaging.event.OrdenCompraEvent;
import lombok.RequiredArgsConstructor;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class EnvelopeProcessor {
    private static final java.util.regex.Pattern CORRELATION = java.util.regex.Pattern.compile("[A-Za-z0-9._-]{1,100}");
    private final ObjectMapper mapper;
    private final VentaConfirmadaHandler handler;

    public boolean procesar(byte[] bytes) {
        JsonNode node;
        try { node = mapper.readTree(bytes); }
        catch (tools.jackson.core.JacksonException e) { throw new EventoInvalidoException("JSON invalido", e); }
        if (node == null || !node.isObject() || !node.hasNonNull("eventType"))
            throw new EventoInvalidoException("Evento sin tipo");
        if (!"VentaConfirmada".equals(node.path("eventType").asText())) return false;
        if (!node.hasNonNull("eventId") || !node.hasNonNull("schemaVersion")
                || node.path("schemaVersion").asInt() != 1 || !node.hasNonNull("payload"))
            throw new EventoInvalidoException("Contrato de venta invalido");
        OrdenCompraEvent event;
        try { event = mapper.treeToValue(node.path("payload"), OrdenCompraEvent.class); }
        catch (tools.jackson.core.JacksonException e) { throw new EventoInvalidoException("Payload invalido", e); }
        String eventId = node.path("eventId").asText();
        String candidate = node.path("correlationId").isTextual() ? node.path("correlationId").asText() : null;
        String correlation = candidate != null && CORRELATION.matcher(candidate).matches() ? candidate
                : CORRELATION.matcher(eventId).matches() ? eventId : java.util.UUID.randomUUID().toString();
        String previous = MDC.get("correlationId");
        try {
            MDC.put("correlationId", correlation);
            handler.procesar(eventId, event);
        } finally {
            if (previous == null) MDC.remove("correlationId"); else MDC.put("correlationId", previous);
        }
        return true;
    }
}
