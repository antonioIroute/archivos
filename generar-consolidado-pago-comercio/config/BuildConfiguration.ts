import { GetParameterCommand, SSMClient } from "@aws-sdk/client-ssm";

export interface AppConfig {
    stage: string;
    CRON: string;
    bucketGlueName: string;
    stackName: string;
    subnetId: string;
    securityGroupIdList: string[];
    JDBC_CONNECTION_URL: string;
    SECRET_ID: string;
}

export class BuildConfiguration {
    private readonly name: string;
    private readonly stage: string;

    constructor(name: string, stage: string) {
        this.name = name;
        this.stage = stage;
    }

    public async getConfig(): Promise<AppConfig> {
        try {
            const parameterName = `/${this.name}/${this.stage}`;

            // * Se obtiene la configuracion desde SSM 
            const ssm = new SSMClient({ region: 'us-east-1' });
            const command = new GetParameterCommand({ Name: parameterName });
            const ssmResponse = await ssm.send(command);

            if (!ssmResponse?.Parameter?.Value) {
                throw new Error("No se obtuvo valor desde SSM. El parámetro está vacío o no existe.");
            }
            // * Se parsea el string como JSON
            const configObject = JSON.parse(ssmResponse.Parameter.Value);

            // *Validamos y construimos el objeto de respuesta
            const buildConfigResponse: AppConfig = {
                stage: this.stage,
                CRON: this.ensureString(configObject, "CRON"),
                bucketGlueName: this.ensureString(configObject, "bucketGlueName"),
                subnetId: this.ensureString(configObject, "subnetId"),
                securityGroupIdList: this.ensureStringArray(configObject, "securityGroupIdList"),
                stackName: this.ensureString(configObject, "stackName"),
                JDBC_CONNECTION_URL: this.ensureString(configObject, "JDBC_CONNECTION_URL"),
                SECRET_ID: this.ensureString(configObject, "SECRET_ID"),
            };
            return buildConfigResponse;
        }catch (error) {
            console.error("Error al obtener configuracion de SSM: ",error);
            throw error; // * Se detiene el despligue si no obtiene la configuracion
        }
    }

    private ensureString(object: { [name: string]: any }, propName: string): string {
        if (!object[propName] || typeof object[propName] !== 'string' || object[propName].trim().length === 0) {
            throw new Error(`La propiedad "${propName}" no existe, está vacía o no es un string en el parámetro de SSM.`);
        }
        return object[propName];
    }

    private ensureStringArray(object: { [name: string]: any }, propName: string): string[] {
        const value = object[propName];
        if (!value || !Array.isArray(value) || value.length === 0) {
            throw new Error(`La propiedad "${propName}" no existe, está vacía o no es un array.`);
        }
        return value;
    }
}