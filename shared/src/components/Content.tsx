import { ArrowForwardIcon } from "@chakra-ui/icons";
import {
  Accordion,
  AccordionButton,
  AccordionIcon,
  AccordionItem,
  AccordionPanel,
  Box,
  Button,
  Text,
} from "@chakra-ui/react";

export const Content = () => {
  return (
    <Box margin="0 auto" maxWidth="75%">
      <Accordion variant="custom">
        <AccordionItem>
          <AccordionButton>
            <Box as="span" flex="1" textAlign="left">
              <Text fontSize="xl" fontWeight="medium" color="#f0f3fa">
                Cloud-Native Scalability & High-Availability
              </Text>
            </Box>
            <AccordionIcon />
          </AccordionButton>
          <AccordionPanel pb={4}>
            <Text fontSize="sm" color="#f0f3fa">
              Enterprise-grade Managed Cloud. Vertical and horizontal scaling
              and zero-downtime upgrades.
            </Text>
            <Button colorScheme="red" size="sm" variant="link" data-tracking>
              Qdrant Cloud <ArrowForwardIcon />
            </Button>
          </AccordionPanel>
        </AccordionItem>

        <AccordionItem>
          <Text>
            <AccordionButton>
              <Box as="span" flex="1" textAlign="left" fontWeight="medium">
                <Text fontSize="xl" color="#f0f3fa">
                  Ease of Use & Simple Deployment
                </Text>
              </Box>
              <AccordionIcon />
            </AccordionButton>
          </Text>
          <AccordionPanel pb={4}>
            <Text fontSize="sm">
              Quick deployment in any environment with Docker and a lean API for
              easy integration, ideal for local testing.
            </Text>
            <Button colorScheme="red" size="sm" variant="link" data-tracking>
              Quick Start Guide <ArrowForwardIcon />
            </Button>
          </AccordionPanel>
        </AccordionItem>
      </Accordion>
    </Box>
  );
};
