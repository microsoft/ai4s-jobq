AI for Science JobQ
===================

The main documentation is hosted at [msr-ai4science.github.io/ai4s-jobq](https://msr-ai4science.github.io/ai4s-jobq/). You can use [aka.ms/jobq](https://aka.ms/jobq) as a short link to this page.


# Installation

For now, installation is done via pip after cloning the repository. We're
working on publishing the package to an index as well.

```bash
git clone https://github.com/msr-ai4science/ai4s-jobq
cd ai4s-jobq
pip install [-e] .
```

The `ai4s.jobq` package enables multiple users to push work items to an [Azure Queue](https://azure.microsoft.com/en-us/products/storage/queues/), while one or more workers pull and process tasks asynchronously. This approach is useful in scenarios where:

- Tasks are too small to justify the overhead of launching an Azure ML job for each one.
- Workloads need to be distributed across diverse environments (e.g., Azure ML clusters in different regions).
- Throughput control is desired, scaling workers up or down as needed.

By decoupling job creation from execution, `ai4s.jobq` allows users to queue up tasks in advance and process them at a controlled rate based on resource availability.

## Key Features
- **Native Azure Queues**: Uses Azure Storage queues without requiring additional services.
- **Robustness**: Jobs automatically reappear in the queue if a worker fails to complete them (for example, after pre-emptions or crashes).
- **Simple CLI Usage**: 
  ```bash
  ai4s-jobq $my_storage_account/$my_unique_queue_name push -c "echo hello"
  ai4s-jobq $my_storage_account/$my_unique_queue_name worker
  ```
  *(Requires Storage Queue Data Contributor role on the selected storage account.)*
- **Advanced Python API**: Efficient handling of I/O-bound tasks, minimizing overhead in blob storage interactions and reducing the need for manual multi-threading/multi-processing.
- **Scalability & Efficiency**: Enables large-scale distributed batch processing while being able to rely on cheap and available pre-emptible compute.
- **Observability**: Workers can transmit telemetry which powers a Grafana dashboard to monitor queue progress.

## **AI for Science: Powering Large-Scale Research**  

`ai4s.jobq` is a **critical tool** in AI for Science, enabling researchers to handle massive computational workloads with ease. It plays a key role in:  

🔹 **Generating large-scale synthetic datasets** for AI-driven simulations.  
🔹 **Efficiently pre- and post-processing** vast amounts of scientific data.  
🔹 **Scaling model evaluation** by managing high-throughput inference workloads.  

### **Why AI for Science Relies on `ai4s.jobq`**  

🚀 **Maximizing Compute Efficiency**  
By seamlessly leveraging **preemptible compute across diverse environments**, `ai4s.jobq` significantly boosts scalability while reducing costs—accelerating scientific discovery without wasted resources.  

🛠 **Focusing on Science, Not Infrastructure**  
Researchers can **stay focused on their work** instead of dealing with unreliable infrastructure. `ai4s.jobq` abstracts away system failures and optimizes task execution, **freeing up valuable time** for breakthroughs in AI and science.  
