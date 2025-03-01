const mysql = require('mysql');
const xlsx = require('xlsx');
require('dotenv').config();

const dbConfig = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD
};

// Crear conexión sin base de datos para poder crearla
const connection = mysql.createConnection(dbConfig);

connection.connect((err) => {
    if (err) {
        console.error('Error de conexión:', err.stack);
        return;
    }
    console.log('Conectado al servidor MySQL.');
});

// Crear la base de datos si no existe
connection.query(`CREATE DATABASE IF NOT EXISTS ${process.env.DB_SCHEMA}`, (err) => {
    if (err) {
        console.error('Error al crear la base de datos:', err);
        return;
    }
    console.log(`Esquema '${process.env.DB_SCHEMA}' creado/verificado.`);
});

// Cerrar la conexión inicial
connection.end(() => {
    console.log("Conexión cerrada. Se procederá a conectar con la base de datos.");
    
    // Ahora conectarse incluyendo la base de datos
    const dbConnection = mysql.createConnection({ ...dbConfig, database: process.env.DB_SCHEMA });

    dbConnection.connect((err) => {
        if (err) {
            console.error('Error al conectar con la base de datos:', err.stack);
            return;
        }
        console.log(`Conectado a la base de datos '${process.env.DB_SCHEMA}'.`);
    });

    // Crear la tabla si no existe
    dbConnection.query(`
        CREATE TABLE IF NOT EXISTS ventas_videojuegos_1 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            titulo VARCHAR(255),
            plataforma VARCHAR(255),
            anio INT,
            genero VARCHAR(255),
            editorial VARCHAR(255),
            ventasNA FLOAT,
            ventasEU FLOAT,
            ventasJP FLOAT,
            ventas_otros FLOAT,
            ventas_global FLOAT
        )
    `, (err) => {
        if (err) {
            console.error('Error al crear la tabla:', err);
            return;
        }
        console.log("Tabla 'ventas_videojuegos_1' verificada.");
    });

    // Leer el archivo Excel
    const filePath = 'C:/Users/Usuario/Downloads/Ventas+Videojuegos-1.xlsx';
    const workbook = xlsx.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    let data = xlsx.utils.sheet_to_json(sheet);

    // Normalizar y limpiar los datos
    const cleanedData = data.map(row => ({
        Nombre: row['Nombre']?.toString().trim() || '',
        Plataforma: row['Plataforma']?.toString().trim() || '',
        Anio: row['Año'] ? parseInt(row['Año'], 10) : null,
        Genero: row['Genero']?.toString().trim() || '',
        Editorial: row['Editorial']?.toString().trim() || '',
        VentasNA: row['Ventas NA'] ? parseFloat(row['Ventas NA']) : 0,
        VentasEU: row['Ventas EU'] ? parseFloat(row['Ventas EU']) : 0,
        VentasJP: row['Ventas JP'] ? parseFloat(row['Ventas JP']) : 0,
        Ventas_Otros: row['Ventas Otros'] ? parseFloat(row['Ventas Otros']) : 0,
        Ventas_Global: row['Ventas Global'] ? parseFloat(row['Ventas Global']) : 0
    }));

    console.log("Datos limpios a insertar:", cleanedData.slice(0, 5));

    // Insertar los datos en la base de datos
    cleanedData.forEach(row => {
        dbConnection.query(
            `INSERT INTO ventas_videojuegos_1 (titulo, plataforma, anio, genero, editorial, ventasNA, ventasEU, ventasJP, ventas_otros, ventas_global) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [row.Nombre, row.Plataforma, row.Anio, row.Genero, row.Editorial, row.VentasNA, row.VentasEU, row.VentasJP, row.Ventas_Otros, row.Ventas_Global],
            (err) => {
                if (err) console.error('Error al insertar datos:', err);
            }
        );
    });

    console.log("Datos insertados correctamente en la base de datos.");
    dbConnection.end();
});
