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

const OutputSchema = Type.Object({
    id: Type.Number(),
    latitude: Type.Number(),
    longitude: Type.Number(),
    deviceId: Type.Integer(),
    utcDate: Type.String({ format: 'date-time' }),
    speed: Type.Number(),
    azimuth: Type.Number(),
    altitude: Type.Number(),
    signal: Type.Number(),
    battery: Type.Number(),
    isOn: Type.Boolean(),
    isIdle: Type.Boolean(),
    tanks: Type.Unknown(),
    thermometers: Type.Unknown(),
    events: Type.Array(Type.Unknown())
})

export default class Task extends ETL {
    static name = 'etl-optimus'
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


        const res = await fetch(`https://api3p.optimustracking.com/v1/clients/${env.OptimusClientID}/position/latest`, {
            headers: {
                Accept: 'application/json',
                'api-key': env.OptimusAPIToken
            }
        });

        const devices = await res.typed(Type.Array(OutputSchema))

        const future = new Date();
        future.setTime(future.getTime() + 172800000);

        for (const device of devices) {
            if (new Date(device.utcDate) < future) {
                fc.features.push({
                    id: device.id,
                    type: 'Feature',
                    properties: {
                        speed: device.speed,
                        course: device.course,
                        metadata: device,
                    },
                    geometry: {
                        type: 'Point',
                        coordinates: [
                            device.longitude,
                            device.latitude,
                            device.altitude
                        ]
                    }
                });
            }
        }

        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

