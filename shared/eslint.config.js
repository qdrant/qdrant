import eslint from "@eslint/js";
import tseslint from "typescript-eslint";
import hooksPlugin from "eslint-plugin-react-hooks";

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  ...tseslint.configs.stylisticTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        projectService: {
          allowDefaultProject: ["eslint.config.*"],
        },
      },
    },

    plugins: {
      "react-hooks": hooksPlugin,
    },

    rules: {
      ...hooksPlugin.configs.recommended.rules,
      "@typescript-eslint/consistent-type-definitions": "off",
      "@typescript-eslint/no-non-null-assertion": "off",
      "@typescript-eslint/no-misused-promises": [
        "error",
        {
          checksVoidReturn: false,
        },
      ],
    },
  },
);
