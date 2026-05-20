package com.dispatch.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Global exception handler for the Dispatch Service HTTP layer.
 *
 * <p>Maps Spring validation exceptions to HTTP 422 with structured error bodies,
 * and oversized request bodies to HTTP 413 (Requirement 3.11, 10.8, 10.9).
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * Handles {@code @Valid} validation failures — returns HTTP 422 with a
     * structured list of field errors.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidationErrors(
            MethodArgumentNotValidException ex) {

        List<Map<String, String>> errors = ex.getBindingResult()
            .getFieldErrors()
            .stream()
            .map(fe -> Map.of(
                "field",   fe.getField(),
                "message", fe.getDefaultMessage() != null ? fe.getDefaultMessage() : "invalid"
            ))
            .collect(Collectors.toList());

        return ResponseEntity
            .status(HttpStatus.UNPROCESSABLE_ENTITY)
            .body(Map.of("errors", errors));
    }

    /**
     * Handles oversized request bodies — returns HTTP 413.
     */
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<Map<String, String>> handleMaxUploadSizeExceeded(
            MaxUploadSizeExceededException ex) {
        return ResponseEntity
            .status(HttpStatus.REQUEST_ENTITY_TOO_LARGE)
            .body(Map.of("error", "request body too large"));
    }
}
