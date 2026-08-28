# Job.Hangfire.Raven6x

[![NuGet Version](https://img.shields.io/nuget/v/Job.Hangfire.Raven6x.svg?style=flat-square)](https://www.nuget.org/packages/Job.Hangfire.Raven6x/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Job.Hangfire.Raven6x.svg?style=flat-square)](https://www.nuget.org/packages/Job.Hangfire.Raven6x/)
[![License: LGPL v3](https://img.shields.io/badge/License-LGPL_v3-blue.svg?style=flat-square)](https://www.gnu.org/licenses/lgpl-3.0)
[![.NET](https://img.shields.io/badge/.NET-7.0%20%7C%208.0%20%7C%209.0-512BD4?style=flat-square&logo=dotnet)](https://dotnet.microsoft.com/)
[![RavenDB](https://img.shields.io/badge/RavenDB-6.x-red?style=flat-square&logo=ravendb)](https://ravendb.net/)

**Job.Hangfire.Raven6x** is a high-performance, robust RavenDB 6.x storage provider for [Hangfire](https://www.hangfire.io/). It enables Hangfire to persist background jobs, recurring tasks, queues, states, and distributed locks directly inside RavenDB.

---

## Features

- ⚡ **High-Performance Batched Operations**: Uses lazy batch loading and statistics queries to eliminate memory bottlenecks on dashboard metrics and multi-queue lookups.
- 🔒 **Distributed Locking with Optimistic Concurrency**: Prevents race conditions across clustered workers using RavenDB optimistic concurrency.
- 📦 **Atomic Patches**: Utilizes RavenDB deferred JavaScript patches for high-throughput counters, set operations, and queue mutations.
- ⏱️ **Automatic Expiration & TTL**: Native document expiration support for completed/expired jobs, stats, and locks.
- 📊 **Full Hangfire Dashboard Integration**: Comprehensive metrics monitoring for enqueued, processing, succeeded, scheduled, failed, and deleted jobs.

---

## Installation

Install the package from [NuGet](https://www.nuget.org/packages/Job.Hangfire.Raven6x/):

### .NET CLI
```bash
dotnet add package Job.Hangfire.Raven6x
```

### Package Manager
```powershell
Install-Package Job.Hangfire.Raven6x
```

---

## Configuration & Usage

### 1. ASP.NET Core Integration

In your `Program.cs` or `Startup.cs`:

```csharp
using Hangfire;
using Hangfire.Raven;
using Hangfire.Raven.Storage;

var builder = WebApplication.CreateBuilder(args);

// Configure Hangfire with RavenDB Storage
builder.Services.AddHangfire(config =>
{
    config.SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
          .UseSimpleAssemblyNameTypeSerializer()
          .UseRecommendedSerializerSettings()
          .UseRavenStorage("http://localhost:8080", "HangfireDB", new RavenStorageOptions
          {
              InvisibilityTimeout = TimeSpan.FromMinutes(30),
              QueuePollInterval = TimeSpan.FromSeconds(2),
              DistributedLockLifetime = TimeSpan.FromMinutes(1)
          });
});

// Add Hangfire Server
builder.Services.AddHangfireServer(options =>
{
    options.WorkerCount = Environment.ProcessorCount * 5;
    options.Queues = new[] { "critical", "default", "low" };
});

var app = builder.Build();

// Enable Hangfire Dashboard
app.UseHangfireDashboard("/hangfire");

app.Run();
```

### 2. Standalone / Console Application

```csharp
using Hangfire;
using Hangfire.Raven;

GlobalConfiguration.Configuration
    .UseRavenStorage("http://localhost:8080", "HangfireDB");

using (var server = new BackgroundJobServer())
{
    Console.WriteLine("Hangfire Server started. Press any key to exit...");
    Console.ReadKey();
}
```

---

## Creating Background Jobs

### Fire-and-Forget Jobs
Executes once, immediately in the background:
```csharp
var jobId = BackgroundJob.Enqueue(() => Console.WriteLine("Fire-and-forget job executed!"));
```

### Delayed Jobs
Executes after a specified delay or at a future time:
```csharp
var jobId = BackgroundJob.Schedule(
    () => Console.WriteLine("Delayed job executed!"),
    TimeSpan.FromDays(7));
```

### Recurring Jobs
Executes repeatedly according to a CRON schedule:
```csharp
RecurringJob.AddOrUpdate(
    "daily-report",
    () => Console.WriteLine("Daily report generated!"),
    Cron.Daily);
```

### Continuations
Chains dependent jobs that run automatically when the parent job finishes:
```csharp
var parentJobId = BackgroundJob.Enqueue(() => Console.WriteLine("Parent task completed!"));
BackgroundJob.ContinueJobWith(parentJobId, () => Console.WriteLine("Continuation task executed!"));
```

---

## Configuration Options

You can customize the storage behavior by passing `RavenStorageOptions`:

```csharp
var options = new RavenStorageOptions
{
    // Interval between queue polls when no items are pending
    QueuePollInterval = TimeSpan.FromSeconds(2),

    // Invisibility timeout before an unacknowledged fetched job is made available again
    InvisibilityTimeout = TimeSpan.FromMinutes(30),

    // Lifetime of distributed locks before automatic expiry/cleanup
    DistributedLockLifetime = TimeSpan.FromMinutes(1),

    // Unique client identifier for lock tracking
    ClientId = Guid.NewGuid().ToString()
};
```

---

## Development & Testing

### Prerequisites
- [.NET 7.0 SDK](https://dotnet.microsoft.com/) or higher (.NET 8.0 / .NET 9.0 supported)
- [RavenDB 6.x](https://ravendb.net/download) instance or Docker container

### Building the Project
```bash
git clone https://github.com/kelvinaxhcar/Job-Hangfire-RavenDB.git
cd Job-Hangfire-RavenDB
dotnet build
```

### Running Tests
```bash
dotnet test
```

---

## Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/kelvinaxhcar/Job-Hangfire-RavenDB/issues) and read the [CONTRIBUTING.md](CONTRIBUTING.md) guide before opening a PR.

---

## License

This project is licensed under the **GNU Lesser General Public License v3.0 (LGPL-3.0)**. See the [LICENSE](LICENSE) file for more details.
