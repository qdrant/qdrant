import {
  Alert,
  AlertDescription,
  Button,
  useToast,
  Wrap,
  WrapItem,
  Text,
  ToastId,
} from "@chakra-ui/react";
import { useEffect } from "react";

export const CookieBanner = () => {
  const toast = useToast();

  useEffect(() => {
    let toastId: ToastId;
    const id = setTimeout(() => {
      toastId = toast({
        render: (_props) => (
          <Alert status="warning" gap={2}>
            <AlertDescription flex="0 1 auto">
              <Text fontSize="sm">
                We use cookies to learn more about you. At any time you can
                delete or block cookies through your browser settings.
              </Text>
            </AlertDescription>
            <Wrap spacing={2} flex="1 0 auto">
              <WrapItem>
                <Button
                  variant="outline"
                  colorScheme="yellow"
                  size="xs"
                  data-tracking
                >
                  Reject
                </Button>
              </WrapItem>
              <WrapItem>
                <Button
                  variant="solid"
                  colorScheme="green"
                  size="xs"
                  data-tracking
                >
                  Accept
                </Button>
              </WrapItem>
            </Wrap>
          </Alert>
        ),

        status: "info",
        duration: null,
        isClosable: true,
      });
    }, 1000);
    return () => {
      clearTimeout(id);
      toast.close(toastId);
    };
  });

  return null;
};
