const toOpenApi = require('@openapi-contrib/json-schema-to-openapi-schema');

const fs = require('fs');

let rawdata = fs.readFileSync('schema.json');

let schema = JSON.parse(rawdata);

// Generated result contains usage of "allOf" directive with a single class.
// It breaks client generator serves no function.
// This function should replace "allOf" directives with simple class usages
function replaceAllOf(schema) {
    if (Array.isArray(schema)) {
        var newSchema = [];
        for (var k in schema) {
            newSchema[k] = replaceAllOf(schema[k]);
        }
        return newSchema;
    }
    if (typeof schema === 'object' && schema !== null) {
        var newSchema = {};
        for (var k in schema) {
            if (k === 'allOf' && schema[k].length === 1) {
                newSchema = {...schema[k][0]};
                break
            } else {
                newSchema[k] = replaceAllOf(schema[k]);
            }
        }
        return newSchema;
    }
    return schema;
}



(async () => {
    var convertedSchema = await toOpenApi(schema);

    convertedSchema = replaceAllOf(convertedSchema);

    for (var modelName in convertedSchema['definitions']) {
        convertedSchema['definitions'][modelName]["$schema"] = schema["$schema"];
        convertedSchema['definitions'][modelName] = await toOpenApi(convertedSchema['definitions'][modelName]);
    }

    console.log(JSON.stringify({components: {schemas: convertedSchema['definitions']}}, null, 4));
})();




