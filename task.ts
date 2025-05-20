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

const Position = Type.Object({
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
})

const Device = Type.Object({
    id: Type.Integer(),
    clientId: Type.Integer(),
    pin: Type.String(),
    description: Type.String(),
    utcOffsetMinutes: Type.Integer(),
    extra: Type.Record(Type.String(), Type.String())
});

const OutputSchema = Position;

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

        const positions = await res.typed(Type.Array(Position))

        const devRes = await fetch(`https://api3p.optimustracking.com/v1/clients/${env.OptimusClientID}/devices`, {
            headers: {
                Accept: 'application/json',
                'api-key': env.OptimusAPIToken
            }
        });

        const devices = await devRes.typed(Type.Array(Device))

        const deviceMap = new Map<number, Static<typeof Device>>();
        devices.forEach((device) => {
            deviceMap.set(device.id, device);
        });

        const future = new Date();
        future.setTime(future.getTime() + 172800000);

        for (const position of positions) {
            if (new Date(position.utcDate) < future) {
                const device = deviceMap.get(position.deviceId);

                fc.features.push({
                    id: String(position.deviceId),
                    type: 'Feature',
                    properties: {
                        type: 'a-h-G',
                        how: 'm-g',
                        callsign: device.description || 'Optimus',
                        speed: position.speed * 0.277778,
                        course: position.azimuth,
                        metadata: position
                    },
                    geometry: {
                        type: 'Point',
                        coordinates: [
                            position.longitude,
                            position.latitude,
                            position.altitude
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

