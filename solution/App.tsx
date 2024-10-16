import { Outlet, ScrollRestoration } from "react-router-dom";
import { ChakraProvider, GlobalStyle } from "@chakra-ui/react";
import useDebugRender from "tilg";
import { TrackingProvider } from "./TrackingProvider";
import { theme } from "./styles/theme";
import { CookieBanner } from "./components/CookieBanner";

export default function App() {
  useDebugRender();

  return (
    <ChakraProvider theme={theme}>
      <GlobalStyle />
      <TrackingProvider>
        <Outlet />
        <CookieBanner />
      </TrackingProvider>
      <ScrollRestoration />
    </ChakraProvider>
  );
}
