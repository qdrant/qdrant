import { Box, Button, Text } from "@chakra-ui/react";

export const HeroHeader = () => {
  return (
    <Box
      paddingTop="2rem"
      position="relative"
      background="url(https://qdrant.tech/img/stars-pattern.png)"
      textAlign="center"
    >
      <Box
        display="flex"
        justifyContent="center"
        flexDirection="column"
        alignItems="center"
        gap="4"
        padding="4rem 0 0"
        textAlign="center"
        maxWidth="100%"
        width="100%"
      >
        <Text fontSize="4xl" fontWeight="semibold" color="#f0f3fa">
          High-Performance Vector Search at Scale
        </Text>
        <Text
          fontSize="lg"
          fontWeight="medium"
          color="#8f98b2"
          marginBottom="8"
        >
          Powering the next generation of AI applications with advanced,
          open-source vector similarity search technology.
        </Text>
        <Button data-tracking>Get started</Button>
      </Box>
      <img
        style={{
          margin: "0 auto",
          display: "block",
          width: "100%",
          maxWidth: "1200px",
          height: "auto",
          top: "-10vh",
          position: "relative",
          pointerEvents: "none",
        }}
        src="https://qdrant.tech/img/hero-home-illustration-x1.png"
      />
    </Box>
  );
};
