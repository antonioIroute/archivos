module.exports = {
  // --- Configuración Base ---
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/test'], // Asume que tus pruebas están en la carpeta 'test'
  testMatch: ['**/*.test.ts'], // Busca archivos que terminen en .test.ts

  // --- Configuración de Cobertura ---
  collectCoverage: true, // ¡La clave! Habilita la cobertura por defecto.
  coverageDirectory: './report/cobertura', // Carpeta de salida para el reporte de cobertura.
  coverageReporters: ['html', 'text', 'cobertura'], // Formatos de cobertura (html para ver en navegador, text en consola, cobertura.xml para SonarQube)

  // --- Configuración de Reporteros ---
  reporters: [
    // 1. Reportero por defecto para la consola
    'default',

    // 2. Reportero JUnit para integración con pipelines (CI/CD)
    [
      'jest-junit',
      {
        outputDirectory: 'report', // Carpeta de salida
        outputName: 'report.xml',    // Nombre del archivo
      },
    ],

    // 3. Reportero HTML para una visualización amigable de los resultados
    ['jest-html-reporters', {
      publicPath: './report', // Carpeta de salida
      filename: 'pruebas_unitarias.html', // Nombre del archivo
      pageTitle: "Reporte de Pruebas - MPG Consolidado Pago Comercio", // Título personalizado para tu proyecto
      expand: true, // Expande los detalles de las pruebas por defecto
    }]
  ]
};