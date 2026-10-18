declare const uncheckedValue: unique symbol

export interface Unchecked<T> {
    /** @ignore Stops typescript-json-schema from walking into T. */
    readonly [uncheckedValue]: T
}

/**
 * Use this to opt out of adding the type to the contract. Useful for complicated types that you would prefer to be opaque.
 * ex: Pi harness message types.
 */
export function unchecked<T>(value: T): Unchecked<T> {
    return value as unknown as Unchecked<T>
}
