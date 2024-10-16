import { Outlet, ScrollRestoration } from "react-router-dom";
import { ChakraProvider, GlobalStyle } from "@chakra-ui/react";
// import useDebugRender from "tilg";
import { theme } from "./styles/theme";
import { CookieBanner } from "./components/CookieBanner";

export default function App() {
  // useDebugRender();

  return (
    <ChakraProvider theme={theme}>
      <GlobalStyle />
      <Outlet />
      <CookieBanner />
      <ScrollRestoration />
    </ChakraProvider>
  );
}
