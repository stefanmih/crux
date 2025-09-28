package com.crux.cli;

/**
 * Represents a user facing error while executing a CLI command. The
 * message should be concise and explain how the user can recover. The
 * original cause (if any) can be attached for debugging but will not be
 * shown to the end user.
 */
class CliException extends RuntimeException {
    CliException(String message) {
        super(message);
    }

    CliException(String message, Throwable cause) {
        super(message, cause);
    }
}
