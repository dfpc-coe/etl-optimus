import { Static, Type, TSchema } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, InputFeatureCollection, DataFlowType, InvocationType } from '@tak-ps/etl';
import { fetch } from '@tak-ps/etl';

const InputSchema = Type.Object({
    OptimusClientID: Type.String(),
    OptimusAPIToken: Type.String(),
    DEBUG: Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

const OutputSchema = Type.Object({})

export default class Task extends ETL {
    static name = 'default'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return InputSchema;
            } else {
                return OutputSchema;
            }
        } else {
            return Type.Object({});
        }
    }

    async control(): Promise<void> {
        const env = await this.env(InputSchema);

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        }

        await fetch(`https://api3p.optimushn.com/api/v1/${env.OptimusClientID}/position/latest`, {
            headers: {
                'api-key': env.OptimusAPIToken
            }
        });

        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

