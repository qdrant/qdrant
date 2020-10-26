const toOpenApi = require('@openapi-contrib/json-schema-to-openapi-schema');

const fs = require('fs');

let rawdata = fs.readFileSync('schema.json');

let schema = JSON.parse(rawdata);

(async () => {
    var convertedSchema = await toOpenApi(schema);

    for (var modelName in convertedSchema['definitions']) {
        convertedSchema['definitions'][modelName]["$schema"] = schema["$schema"];
        convertedSchema['definitions'][modelName] = await toOpenApi(convertedSchema['definitions'][modelName]);
    }

    console.log(JSON.stringify({components: {schemas: convertedSchema['definitions']}}, null, 4));
})();




