AI for Science JobQ
===================

The main documentation is hosted at [microsoft.github.io/ai4s-jobq](https://microsoft.github.io/ai4s-jobq/).


# Installation

To install, run

```bash
pip install ai4s-jobq

# or, if you log data to app insights and want to use a local dashboard:
pip install ai4s-jobq[track]
```

The `ai4s.jobq` package enables multiple users to push work items to an [Azure Queue](https://azure.microsoft.com/en-us/products/storage/queues/) or an [Azure Servicebus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-overview), while one or more workers pull and process tasks asynchronously. This approach is useful in scenarios where:

- Tasks are too small to justify the overhead of launching an Azure ML job for each one.
- Workloads need to be distributed across diverse environments (e.g., Azure ML clusters in different regions).
- Throughput control is desired, scaling workers up or down as needed.

By decoupling job creation from execution, `ai4s.jobq` allows users to queue up tasks in advance and process them at a controlled rate based on resource availability.

# Key Features
- **Native Azure Queues**: Uses Azure Storage queues or Servicebus, no additional infrastructure.
- **Robustness**: Jobs automatically reappear in the queue if a worker fails to complete them (for example, after pre-emptions or crashes).
- **Simple CLI Usage**:
  ```bash
  # Azure Storage Queue
  export QUEUE=my_storage_account_name/my_queue_name
  # ...or Azure Servicebus (pick one!)
  export QUEUE=sb://my_service_bus/my_queue_name

  ai4s-jobq $QUEUE push -c "echo hello"
  ai4s-jobq $QUEUE worker
  ```
  *(Requires Storage Queue Data Contributor role on the selected storage account for Azure Storage Queues or Azure Service Bus Data Owner role for Servicebus.)*
- **Advanced Python API**: Efficient handling of I/O-bound tasks, minimizing overhead in blob storage interactions and reducing the need for manual multi-threading/multi-processing.
- **Scalability & Efficiency**: Enables large-scale distributed batch processing while being able to rely on cheap and available pre-emptible compute.
- **Observability**: Workers can transmit telemetry which powers a Grafana/local dashboard to monitor queue progress.

## **AI for Science: Powering Large-Scale Research**

`ai4s.jobq` is a **critical tool** in Microsoft Research -- AI for Science, enabling researchers to handle massive computational workloads with ease. It plays a key role in:

🔹 **Generating large-scale synthetic datasets** for AI-driven simulations.
🔹 **Efficiently pre- and post-processing** vast amounts of scientific data.
🔹 **Scaling model evaluation** by managing high-throughput inference workloads.

### **Why AI for Science Relies on `ai4s.jobq`**

🚀 **Maximizing Compute Efficiency**
By seamlessly leveraging **preemptible compute across diverse environments**, `ai4s.jobq` significantly boosts scalability while reducing costs—accelerating scientific discovery without wasted resources.

🛠 **Focusing on Science, Not Infrastructure**
Researchers can **stay focused on their work** instead of dealing with unreliable infrastructure. `ai4s.jobq` abstracts away system failures and optimizes task execution, **freeing up valuable time** for breakthroughs in AI and science.  


# Trademarks

This project may contain trademarks or logos for projects, products, or
services. Authorized use of Microsoft trademarks or logos is subject to and
must follow Microsoft’s Trademark & Brand Guidelines. Use of Microsoft
trademarks or logos in modified versions of this project must not cause
confusion or imply Microsoft sponsorship. Any use of third-party trademarks or
logos are subject to those third-party’s policies.
