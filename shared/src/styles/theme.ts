/* eslint-disable @typescript-eslint/unbound-method */
import { accordionAnatomy } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers, extendTheme } from "@chakra-ui/react";

const { definePartsStyle, defineMultiStyleConfig } =
  createMultiStyleConfigHelpers(accordionAnatomy.keys);

const custom = definePartsStyle({
  // panel: {
  //   borderColor: "gray.600",
  //   background: "gray.800",
  // },
  container: {
    borderColor: "gray.700",
    background: "#161e33",
  },
  root: {
    padding: "1.5rem",
    color: "#8f98b2",
    borderColor: "gray.700",
    borderWidth: "1px",
    background: "#161e33",
    borderRadius: "10",
    boxShadow: "0 10px 20px rgba(22,30,51,.1),0 1px 2px rgba(22,30,51,.1)",
  },
  // icon: {
  //   borderColor: "gray.600",
  //   background: "gray.600",
  //   color: "gray.400",
  // },
});

export const accordionTheme = defineMultiStyleConfig({ variants: { custom } });

export const theme = extendTheme({
  styles: {
    global: {
      "html, body": {
        backgroundColor: "#090e1a",
        color: "rgb(40, 50, 77)",
        padding: "0 0 3rem",
      },
      a: {
        color: "rgb(220, 36, 76)",
      },
    },
  },
  components: {
    Accordion: accordionTheme,
  },
});
