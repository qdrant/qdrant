# README

## Welcome to the Qdrant Cloud UI challenge

Hi there! 👋

Thanks for your availability and for joining us today in the upcoming technical challenge.

This file is intended to give you serve as mental preparation and to provide a glimpse into the tasks we'll be working on in a few minutes.

## Preparation

Once we start the video call, we'll be sharing a link to an online editor (choices are VSCode, Intellij and others), where we've setup a small SPA application that will serve as a playground, to implement some technical solutions to a JavaScript (with TypeScript) challenge - within a React application context.

The problem will be shared in more detail within code comments, with a comprenhensive list of TODOs. The entry point for this challenge is the file `src/tracking.tsx`, which will contain the aforementioned details.

## Pair-coding

The idea is that you'll share your screen while we pair-code in this challenge, you'll be the main editor on your side within your browser, and everything is setup to work productively: web dev server with hot module replacement (vite), typescript, eslint, jsx, and routing. Once the environment is run (`npm run vite.dev`), the challenge will start, and we will able to see the results in a second browser tab, one holds the editor and the other the SPA.

## Scope and context

The user story goes as follows: our pages offer a cookie banner which appears flying over the bottom part of the viewport and it contains two action buttons: Reject and Accept. There are other action buttons across the application, which also need attention. You'll find all of the ones that are relevant by searching for the DOM attribute `data-tracking` in the relevant React components (`.jsx`).

As you've probably guessed, we'd like to implement event tracking in the SPA, from clicks to navigation to other routes (though the former is the primary goal of this exercise).

Thanks for participating and see you on the other side in a few!
