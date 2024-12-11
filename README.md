## Table of Contents
- [Table of Contents](#table-of-contents)
- [Overview](#overview)
  - [Block Detection](#block-detection)
  - [Event Processing](#event-processing)
  - [Batch Preparation](#batch-preparation)
  - [Batch Submission](#batch-submission)
- [Architecture](#architecture)
- [Relayer Interaction](#relayer-interaction)
- [Find us](#find-us)

## Overview

![Event Collector](docs/assets/EventCollectorArchitecture.png)

The Event Collector is a key component of the **Submission Sequencer** system, responsible for interacting with the blockchain, processing events from detected blocks, and preparing data for downstream processing. Its primary role is to detect and parse critical events, aggregate event data into batches, and send these batches to the **Finalizer** component for further actions.

By operating seamlessly within the broader system, the Event Collector ensures timely and reliable processing of blockchain data, which is critical for maintaining the integrity and accuracy of the Submission Sequencer.

Key functionalities:

- **Block Detection:** Continuously monitors the blockchain to fetch the latest blocks and process them sequentially.
- **Event Processing:** Extracts and processes logs from the detected blocks, focusing on parsing and storing specific event details such as details from the `EpochReleased` event.
- **Batch Preparation:** Aggregates the event data collected during event processing and organises it into structured batches for submission.
- **Batch Submission:** After batch preparation, the system constructs submission details and pushes these batches into the finalizer queue for further processing.

### Block Detection
The Event Collector actively monitors the blockchain to detect new blocks, maintaining real-time synchronization with the network. It begins by identifying the latest block and processes subsequent blocks in sequential order. Key details, such as block numbers and block hashes, are captured and stored in a distributed storage solution like Redis, enabling efficient event processing and reliable data validation.

### Event Processing
Once a block is detected, the Event Collector extracts and processes the events emitted from the block, focusing on those critical to the Submission Sequencer system, such as the `EpochReleased` event. Using a filter query applied during block detection, the system retrieves logs associated with specific smart contract events. 

These logs are then parsed to extract event parameters, such as the epoch release block number, which is then used to calculate the submission limit block number by adding the submission window. 

```
submissionLimitBlockNumber := epochReleaseBlockNumber + submissionWindow
```

These epoch marker details are then stored in Redis for quick and efficient retrieval.

### Batch Preparation
This process involves monitoring stored epoch marker details to track progress toward submission limits. The Event Collector periodically checks these markers to identify when the current block number aligns with the calculated submission limit block number for a given epoch.

When a match is identified, it indicates the end of the submission window, prompting the system to initiate the batch preparation process. During this stage, collected event data is organized into structured batches, ensuring it is properly formatted and ready to be sent to the finalizer queue for further processing.

### Batch Submission
After the batches are organized, they are iterated over, and the relevant data is transformed into submission details. These details are then placed into the Finalizer Queue for further processing.

The **Finalizer**, an auto-scaled component that follows, retrieves and processes data from the Finalizer Queue, completing the batch processing pipeline. This architecture enables the system to scale effectively, managing submission tasks in parallel and optimizing both performance and throughput.

## Architecture

The Event Collector operates as a singleton component within the codebase, structured around three primary modules that collectively enable its functionality:

1. **Main Module(`cmd/main.go`)**:
   - This serves as the entry point for the Event Collector, orchestrating key operations such as initializing interfaces, starting the API server, detecting blocks, listening for epoch release events, aggregating event data, and preparing submission batches.

2. **Configuration Module (`/config`)**:
   - The `/config` directory houses configuration files that define critical system parameters. These include RPC endpoints, contract addresses, timeout settings, authentication tokens, security parameters, and other project-specific configurations.

3. **Package Module (`/pkgs`)**:
   - The core event processing logic resides in the `/pkgs/prost` directory. These modules handle event processing workflows tailored to different data markets and epochs, forming the backbone of the Event Collector's functionality.

This modular design promotes a clear separation of responsibilities, with each module dedicated to a specific function within the system. Operating as a singleton component, the Event Collector provides a centralized, efficient, and reliable framework for event processing.

## Relayer Interaction

The Event Collector also interacts with the relayer to communicate critical updates, ensuring that all system components are synchronized and informed:

### Batch Processing Updates
- **Batch Size:** Sends real-time updates to the relayer about batch sizes for each data market and epoch combination

### Reward Management
- **Intraday Updates:** Periodic reward updates with slotIDs and eligible submission counts are sent out to the relayer during the day 
- **Day Transition Updates:** After the day transitions, a final update is sent out to the relayer, including the eligible nodes count

## Find us

* [Discord](https://powerloom.io/discord)
* [Twitter](https://twitter.com/PowerLoomHQ)
* [Github](https://github.com/PowerLoom)
* [Careers](https://wellfound.com/company/powerloom/jobs)
* [Blog](https://blog.powerloom.io/)
* [Medium Engineering Blog](https://medium.com/powerloom)