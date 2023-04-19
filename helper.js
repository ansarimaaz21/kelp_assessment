const csv = require('csv-parser');
const fs = require('fs');
const { Transform } = require('stream');

const convertCsvToJson = (csvFilePath) => {
    return new Promise((resolve, reject) => {
        const transformStream = new Transform({
            objectMode: true,
            transform: (data, encoding, callback) => {
                // Since firstName, lastName and age are mandatory fields.
                const record = {
                    name: {
                        firstName: data['name.firstName'],
                        lastName: data['name.lastName']
                    },
                    age: Number(data.age) ? Number(data.age) : 0
                };
                // Add other optional properties from CSV file to the object
                for (const key in data) {
                    if (key !== 'name.firstName' && key !== 'name.lastName' && key !== 'age') {
                        const keys = key.split('.');
                        let obj = record;
                        keys.forEach((k, i) => {
                            if (!obj[k]) obj[k] = {};
                            if (i === keys.length - 1) obj[k] = data[key];
                            obj = obj[k];
                        });
                    }
                }
                callback(null, record);
            }
        });

        const readStream = fs.createReadStream(csvFilePath);
        const chunks = [];
        readStream
            .pipe(csv())
            .pipe(transformStream)
            .on('data', (chunk) => {
                chunks.push(chunk);
            })
            .on('end', () => {
                resolve(chunks);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
};

module.exports = {
    convertCsvToJson
};
