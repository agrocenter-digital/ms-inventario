package com.agrocenter.ms_inventario.messaging.kinesis;

public class EventoInvalidoException extends IllegalArgumentException {
    public EventoInvalidoException(String message) { super(message); }
    public EventoInvalidoException(String message, Throwable cause) { super(message, cause); }
}
