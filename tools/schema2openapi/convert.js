const toOpenApi = require('@openapi-contrib/json-schema-to-openapi-schema');

const fs = require('fs');

let rawdata = fs.readFileSync('schema.json');

let schema = JSON.parse(rawdata);

(async () => {
    const convertedSchema = await toOpenApi(schema);
    console.log(JSON.stringify(convertedSchema, null, 4));
})();




