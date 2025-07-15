#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { MpgMssGenerarConsolidadoPagoComercioStack } from '../lib/mpg-mss-generar-consolidado-pago-comercio-stack';
import { BuildConfiguration } from '../config/BuildConfiguration';

const app = new cdk.App();
const nameStackApplication = `mpg-mss-generar-consolidado-pago-comercio`
const Main = async (app: any) => {
  const stage = app.node.tryGetContext("stage") ?? "dev"
 
  const parameterNameBase = 'mpg-mss-generar-consolidado';
  const configBuilder = new BuildConfiguration(parameterNameBase, stage);
  const appConfig = await configBuilder.getConfig();

  try {
   
    new MpgMssGenerarConsolidadoPagoComercioStack(app, `${nameStackApplication}-${stage}`, {
      stackName: appConfig.stackName,
      config: appConfig,
      env: {
        account: process.env.CDK_DEFAULT_ACCOUNT,
        region: process.env.CDK_DEFAULT_REGION
      },
    });
   
  } catch (e) {
    console.error(e);
  }
 
  app.synth()
}
 
Main(app)