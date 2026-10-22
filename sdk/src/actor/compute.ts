import { z } from "zod"

import { ActorDefinitionError } from "../errors.js"

const computeRegion = z.enum([
    "canada",
    "north-america-east",
    "north-america-central",
    "north-america-south",
    "north-america-west",
    "europe-west",
    "asia-southeast"
])

const cpuCores = z.number().min(0.1).max(64).multipleOf(0.001)
const memoryMiB = z.number().int().min(128).max(262144)

const computeOptionsSchema = z
    .strictObject({
        cpu: cpuCores.optional(),
        cpuLimit: cpuCores.optional(),
        memoryMiB: memoryMiB.optional(),
        memoryLimitMiB: memoryMiB.optional(),
        idleTimeoutMs: z.number().int().min(1).max(86400000).optional(),
        regions: z
            .array(computeRegion)
            .min(1)
            .max(computeRegion.options.length)
            .refine(regions => new Set(regions).size === regions.length, "regions must be unique")
            .optional()
    })
    .refine(({ cpu, cpuLimit }) => cpu === undefined || cpuLimit === undefined || cpu <= cpuLimit, {
        message: "cpu must not exceed cpuLimit",
        path: ["cpuLimit"]
    })
    .refine(
        ({ memoryMiB, memoryLimitMiB }) =>
            memoryMiB === undefined || memoryLimitMiB === undefined || memoryMiB <= memoryLimitMiB,
        { message: "memoryMiB must not exceed memoryLimitMiB", path: ["memoryLimitMiB"] }
    )

type ComputeRegion = z.infer<typeof computeRegion>
interface ComputeOptions {
    /** CPU cores requested, from 0.1 to 64 in increments of 0.001. The server may allow a narrower range. */
    readonly cpu?: number
    /** CPU cap in cores, at least `cpu`. Unset inherits the server default, which may be no cap. */
    readonly cpuLimit?: number
    /** Memory requested, from 128 to 262144 MiB. The server may allow a narrower range. */
    readonly memoryMiB?: number
    /** Memory cap in MiB, at least `memoryMiB`. Unset inherits the server default, raised to `memoryMiB`. */
    readonly memoryLimitMiB?: number
    /** Inactivity before eviction, from 1 to 86400000 ms. Inherits the server default (normally 10000). */
    readonly idleTimeoutMs?: number
    /** Allowed compute regions, without priority order. Existing actors remain in their saved region. */
    readonly regions?: readonly ComputeRegion[]
}

/** Overrides the deployment's compute defaults for this actor class. */
function Compute(options: ComputeOptions) {
    computeOptionsSchema.parse(options)
    return (_value: Function, context: Pick<ClassDecoratorContext, "kind">): void => {
        if (context.kind !== "class") throw new ActorDefinitionError("@Compute requires an actor class")
    }
}

export { Compute, computeOptionsSchema }
export type { ComputeOptions, ComputeRegion }
