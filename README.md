# Go Temporal Module

[![Go Version](https://img.shields.io/badge/go-1.23+-blue.svg)](https://golang.org/doc/devel/release.html)
[![Temporal SDK](https://img.shields.io/badge/temporal-v1.21.2-green.svg)](https://github.com/temporalio/sdk-go)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Timezone](https://img.shields.io/badge/timezone-WIB%20(GMT%2B7)-orange.svg)](https://time.is/WIB)

Sebuah Go module lengkap dan mudah digunakan untuk **Temporal Workflow Engine**. Module ini menyediakan semua fitur yang dibutuhkan untuk membangun aplikasi workflow yang robust, dengan fokus pada kemudahan penggunaan, timezone Indonesia (WIB), dan dokumentasi berbahasa Indonesia.

## 🎯 Fitur Utama

### ✅ Semua Kebutuhan Temporal
- **Manajemen Client**: Koneksi otomatis, health check, konfigurasi mudah
- **Eksekusi Workflow**: Pattern workflow yang siap pakai
- **Manajemen Activity**: Registry activity, heartbeat otomatis  
- **Manajemen Worker**: Multi-worker dengan tuning performa
- **Sistem Scheduling**: Cron jobs, interval, dan one-shot execution
- **Policy Eksekusi**: Penanganan error dan timeout yang fleksibel

### ✅ Fitur Khusus Sesuai Requirement
- **✅ Cron Jobs**: Looping otomatis setiap waktu tertentu
- **✅ One-Shot Execution**: Eksekusi sekali pakai dengan penjadwalan
- **✅ Retry Otomatis**: Retry otomatis ketika gagal (bisa dikonfigurasi)
- **✅ Eksekusi Terjadwal**: Menjalankan workflow di waktu yang ditentukan
- **✅ Policy Kegagalan**: Berhenti otomatis jika gagal (bisa dikonfigurasi)
- **✅ WorkflowID by Parameter**: Generate WorkflowID dengan berbagai strategi

### ✅ Bonus Features
- **Logging Terstruktur**: Integrasi dengan Zap logger
- **Monitoring**: Metrics Prometheus untuk monitoring produksi
- **Testing Utilities**: Tools untuk testing workflow dengan mudah
- **Timezone Indonesia**: Support penuh untuk WIB (GMT+7) - Asia/Jakarta
- **Dokumentasi Lengkap**: Contoh-contoh praktis dan dokumentasi bahasa Indonesia

## 📦 Instalasi

```bash
# Buat project baru atau di project yang sudah ada
go mod init nama-project-anda

# Install module ini
go get github.com/kangkosw/go-mod-temporal
```

## 🌏 Timezone Indonesia (WIB)

Module ini dilengkapi dengan dukungan penuh untuk timezone Indonesia (WIB - GMT+7). Semua timestamp dan penjadwalan otomatis menggunakan timezone Asia/Jakarta.

### Setup Timezone WIB

```go
package main

import (
    "log"
    "time"
)

func main() {
    // Set timezone ke Indonesia (WIB - GMT+7)
    location, err := time.LoadLocation("Asia/Jakarta")
    if err != nil {
        log.Printf("⚠️ Failed to load Asia/Jakarta timezone: %v, using fallback", err)
        location = time.FixedZone("WIB", 7*60*60) // GMT+7
    }
    time.Local = location
    
    // Sekarang semua operasi waktu akan menggunakan WIB
    sekarang := time.Now()
    fmt.Printf("Waktu WIB: %s\n", sekarang.Format("15:04:05 WIB (Monday, 02 Jan 2006)"))
}
```

### Contoh Workflow dengan WIB

```go
func WorkflowDenganWIB(ctx workflow.Context, task string) (string, error) {
    // Workflow ini akan menampilkan waktu dalam WIB
    now := time.Now()
    result := fmt.Sprintf("Task '%s' selesai pada %s", 
        task, now.Format("15:04:05 WIB (Monday, 02 Jan 2006)"))
    
    return result, nil
}
```

## 🚀 Cara Penggunaan

### 1. Setup Dasar

```go
package main

import (
    "context"
    "log"
    "time"
    
    "go.temporal.io/sdk/workflow"
    
    "github.com/kangkosw/go-mod-temporal/client"
    "github.com/kangkosw/go-mod-temporal/worker"
)

// Contoh workflow sederhana
func WorkflowSederhana(ctx workflow.Context, nama string) (string, error) {
    logger := workflow.GetLogger(ctx)
    logger.Info("Workflow dimulai", "nama", nama)
    
    // Simulasi proses
    workflow.Sleep(ctx, 2*time.Second)
    
    hasil := "Halo " + nama + "! Workflow selesai pada " + workflow.Now(ctx).Format("15:04:05")
    logger.Info("Workflow selesai", "hasil", hasil)
    return hasil, nil
}

func main() {
    // 1. Setup client Temporal
    temporalClient, err := client.NewClientWithConfig(&client.Config{
        HostPort:  "localhost:7233",
        Namespace: "default",
    })
    if err != nil {
        log.Fatal("Gagal membuat client:", err)
    }
    defer temporalClient.Close()
    
    // 2. Setup worker
    workerManager := worker.NewManager(temporalClient)
    myWorker, err := workerManager.AddWorker("task-queue-saya", nil)
    if err != nil {
        log.Fatal("Gagal membuat worker:", err)
    }
    
    // 3. Daftarkan workflow
    myWorker.RegisterWorkflow(WorkflowSederhana)
    
    // 4. Jalankan worker
    if err := myWorker.Start(); err != nil {
        log.Fatal("Gagal menjalankan worker:", err)
    }
    defer myWorker.Stop()
    
    log.Println("✅ Worker berhasil dijalankan!")
    
    // 5. Jalankan workflow
    ctx := context.Background()
    workflowRun, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
        ID:        "workflow-test-123",
        TaskQueue: "task-queue-saya",
    }, WorkflowSederhana, "Budi")
    
    if err != nil {
        log.Fatal("Gagal menjalankan workflow:", err)
    }
    
    // 6. Tunggu hasil
    var hasil string
    err = workflowRun.Get(ctx, &hasil)
    if err != nil {
        log.Printf("❌ Workflow gagal: %v", err)
    } else {
        log.Printf("🎉 Workflow berhasil: %s", hasil)
    }
}
```

### 2. Cron Jobs (Looping Otomatis)

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/kangkosw/go-mod-temporal/client"
    "github.com/kangkosw/go-mod-temporal/schedule"
    "github.com/kangkosw/go-mod-temporal/worker"
)

// Workflow untuk laporan harian
func LaporanHarian(ctx workflow.Context, tanggal string) (string, error) {
    logger := workflow.GetLogger(ctx)
    logger.Info("Membuat laporan harian", "tanggal", tanggal)
    
    // Simulasi pembuatan laporan
    workflow.Sleep(ctx, 3*time.Second)
    
    return "Laporan harian untuk " + tanggal + " berhasil dibuat", nil
}

func main() {
    // Setup client dan worker
    temporalClient, err := client.NewClientWithConfig(&client.Config{
        HostPort:  "localhost:7233",
        Namespace: "default",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer temporalClient.Close()
    
    workerManager := worker.NewManager(temporalClient)
    worker, err := workerManager.AddWorker("laporan-harian", nil)
    if err != nil {
        log.Fatal(err)
    }
    
    worker.RegisterWorkflow(LaporanHarian)
    
    if err := worker.Start(); err != nil {
        log.Fatal(err)
    }
    defer worker.Stop()
    
    // Setup cron job - jalan setiap hari jam 9 pagi
    scheduleManager := schedule.NewManager(temporalClient)
    
    cronConfig := &schedule.Config{
        ScheduleID: "laporan-harian-otomatis",
        Spec: &schedule.CronSpec{
            Expression: "0 9 * * *", // 9:00 AM setiap hari
        },
        WorkflowType: "LaporanHarian",
        TaskQueue:    "laporan-harian",
        Args:         []interface{}{time.Now().Format("2006-01-02")},
    }
    
    err = scheduleManager.Create(context.Background(), cronConfig)
    if err != nil {
        log.Printf("⚠️ Gagal membuat cron job: %v", err)
    } else {
        log.Printf("✅ Cron job berhasil dibuat: %s", cronConfig.ScheduleID)
    }
    
    log.Println("Cron job berjalan... Tekan Ctrl+C untuk berhenti")
    select {} // Tetap berjalan
}
```

### 3. One-Shot Execution (Sekali Pakai)

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "go.temporal.io/sdk/client"
    "go.temporal.io/sdk/workflow"
    
    temporalclient "github.com/kangkosw/go-mod-temporal/client"
    "github.com/kangkosw/go-mod-temporal/worker"
    workflowpkg "github.com/kangkosw/go-mod-temporal/workflow"
)

// Workflow untuk mengirim email
func KirimEmail(ctx workflow.Context, penerima string, subjek string, isi string) (string, error) {
    logger := workflow.GetLogger(ctx)
    logger.Info("Mengirim email", "penerima", penerima, "subjek", subjek)
    
    // Simulasi pengiriman email
    workflow.Sleep(ctx, 2*time.Second)
    
    return fmt.Sprintf("Email berhasil dikirim ke %s dengan subjek: %s", penerima, subjek), nil
}

func main() {
    // Setup seperti biasa
    temporalClient, err := temporalclient.NewClientWithConfig(&temporalclient.Config{
        HostPort:  "localhost:7233",
        Namespace: "default",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer temporalClient.Close()
    
    workerManager := worker.NewManager(temporalClient)
    emailWorker, err := workerManager.AddWorker("email-sender", nil)
    if err != nil {
        log.Fatal(err)
    }
    
    emailWorker.RegisterWorkflow(KirimEmail)
    
    if err := emailWorker.Start(); err != nil {
        log.Fatal(err)
    }
    defer emailWorker.Stop()
    
    // Generate WorkflowID unik
    idGenerator := workflowpkg.NewIDGenerator(&workflowpkg.IDConfig{
        Strategy: workflowpkg.UUIDStrategy,
    })
    
    ctx := context.Background()
    
    // 1. Eksekusi langsung (sekarang)
    log.Println("=== Mengirim Email Langsung ===")
    workflowRun, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
        ID:        fmt.Sprintf("email-langsung-%s", idGenerator.Generate()),
        TaskQueue: "email-sender",
    }, KirimEmail, "user@example.com", "Selamat Datang", "Selamat datang di aplikasi kami!")
    
    if err != nil {
        log.Printf("❌ Gagal menjalankan: %v", err)
    } else {
        var hasil string
        err = workflowRun.Get(ctx, &hasil)
        if err != nil {
            log.Printf("❌ Email gagal: %v", err)
        } else {
            log.Printf("✅ %s", hasil)
        }
    }
    
    // 2. Eksekusi terjadwal (5 detik dari sekarang)
    log.Println("\n=== Menjadwalkan Email untuk 5 detik lagi ===")
    waktuKirim := time.Now().Add(5 * time.Second)
    log.Printf("Email akan dikirim pada: %s", waktuKirim.Format("15:04:05"))
    
    scheduledRun, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
        ID:        fmt.Sprintf("email-terjadwal-%s", idGenerator.Generate()),
        TaskQueue: "email-sender",
    }, KirimEmail, "admin@example.com", "Pengingat Terjadwal", "Ini adalah pengingat yang dijadwalkan")
    
    if err != nil {
        log.Printf("❌ Gagal menjadwalkan: %v", err)
    } else {
        log.Println("⏰ Email dijadwalkan, menunggu eksekusi...")
        var hasil string
        err = scheduledRun.Get(ctx, &hasil)
        if err != nil {
            log.Printf("❌ Email terjadwal gagal: %v", err)
        } else {
            log.Printf("✅ %s", hasil)
        }
    }
}
```

### 4. Retry Otomatis (Coba Lagi Jika Gagal)

```go
package main

import (
    "context"
    "fmt"
    "log"
    "math/rand"
    "time"
    
    "go.temporal.io/sdk/client"
    "go.temporal.io/sdk/temporal"
    "go.temporal.io/sdk/workflow"
    
    temporalclient "github.com/kangkosw/go-mod-temporal/client"
    "github.com/kangkosw/go-mod-temporal/worker"
    workflowpkg "github.com/kangkosw/go-mod-temporal/workflow"
)

// Workflow yang mungkin gagal (simulasi koneksi jaringan)
func WorkflowTidakStabil(ctx workflow.Context, namaTask string, tingkatKegagalan float64) (string, error) {
    logger := workflow.GetLogger(ctx)
    logger.Info("Menjalankan task tidak stabil", "task", namaTask, "tingkat_kegagalan", tingkatKegagalan)
    
    // Simulasi kegagalan acak
    if rand.Float64() < tingkatKegagalan {
        err := fmt.Errorf("task %s gagal (simulasi kegagalan jaringan)", namaTask)
        logger.Error("Task gagal", "error", err)
        return "", err
    }
    
    // Simulasi pemrosesan
    workflow.Sleep(ctx, 2*time.Second)
    
    hasil := fmt.Sprintf("Task %s berhasil diselesaikan pada %s", namaTask, workflow.Now(ctx).Format("15:04:05"))
    logger.Info("Task berhasil", "hasil", hasil)
    return hasil, nil
}

func main() {
    rand.Seed(time.Now().UnixNano())
    
    // Setup seperti biasa
    temporalClient, err := temporalclient.NewClientWithConfig(&temporalclient.Config{
        HostPort:  "localhost:7233",
        Namespace: "default",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer temporalClient.Close()
    
    workerManager := worker.NewManager(temporalClient)
    retryWorker, err := workerManager.AddWorker("retry-tasks", nil)
    if err != nil {
        log.Fatal(err)
    }
    
    retryWorker.RegisterWorkflow(WorkflowTidakStabil)
    
    if err := retryWorker.Start(); err != nil {
        log.Fatal(err)
    }
    defer retryWorker.Stop()
    
    idGenerator := workflowpkg.NewIDGenerator(&workflowpkg.IDConfig{
        Strategy: workflowpkg.UUIDStrategy,
    })
    
    ctx := context.Background()
    
    // Retry dengan policy sederhana - maksimal 3 kali percobaan
    log.Println("=== Task dengan Retry Policy ===")
    log.Println("Tingkat kegagalan: 60% (akan retry otomatis jika gagal)")
    
    workflowRun, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
        ID:        fmt.Sprintf("retry-task-%s", idGenerator.Generate()),
        TaskQueue: "retry-tasks",
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts:    3,                    // Maksimal 3 percobaan
            InitialInterval:    1 * time.Second,     // Jeda awal 1 detik
            MaximumInterval:    10 * time.Second,    // Jeda maksimal 10 detik
            BackoffCoefficient: 2.0,                 // Jeda dikalikan 2 setiap retry
        },
    }, WorkflowTidakStabil, "TASK-PENTING", 0.6) // 60% tingkat kegagalan
    
    if err != nil {
        log.Printf("❌ Gagal memulai task: %v", err)
        return
    }
    
    log.Println("⏳ Task berjalan... (akan retry otomatis jika gagal)")
    
    var hasil string
    err = workflowRun.Get(ctx, &hasil)
    if err != nil {
        log.Printf("❌ Task akhirnya gagal setelah retry: %v", err)
    } else {
        log.Printf("✅ Task berhasil: %s", hasil)
    }
}
```

## 📚 Struktur Package

### 🔹 Package Client
**Fungsi**: Mengelola koneksi ke Temporal server  
**Fitur Utama**: 
- Konfigurasi koneksi otomatis
- Health check otomatis
- Manajemen connection pool

```go
// Contoh penggunaan
client, err := client.NewClientWithConfig(&client.Config{
    HostPort:  "localhost:7233",
    Namespace: "default",
})
```

### 🔹 Package Worker  
**Fungsi**: Mengelola worker yang menjalankan workflow  
**Fitur Utama**:
- Multi-worker support
- Performance tuning otomatis
- Lifecycle management

```go
// Contoh penggunaan
workerManager := worker.NewManager(client)
myWorker, err := workerManager.AddWorker("task-queue", &worker.Options{
    MaxConcurrentWorkflowTasks: 100,
})
```

### 🔹 Package Workflow
**Fungsi**: Utilities untuk workflow dan WorkflowID  
**Fitur Utama**:
- Generate WorkflowID dengan berbagai strategi
- Context management
- Workflow utilities

```go
// Contoh penggunaan
idGenerator := workflow.NewIDGenerator(&workflow.IDConfig{
    Strategy: workflow.UUIDStrategy,
})
workflowID := idGenerator.Generate()
```

### 🔹 Package Schedule
**Fungsi**: Sistem penjadwalan untuk workflow  
**Fitur Utama**:
- Cron scheduling
- Interval scheduling  
- One-shot scheduling

```go
// Contoh penggunaan
scheduleManager := schedule.NewManager(client)
err := scheduleManager.Create(ctx, &schedule.Config{
    ScheduleID: "daily-backup",
    Spec: &schedule.CronSpec{Expression: "0 2 * * *"},
})
```

### 🔹 Package Activity
**Fungsi**: Mengelola activity dalam workflow  
**Fitur Utama**:
- Registry activity
- Heartbeat otomatis
- Progress tracking

### 🔹 Package Execution
**Fungsi**: Policy eksekusi dan penanganan error  
**Fitur Utama**:
- Failure policy management
- Timeout configuration
- Error tracking

### 🔹 Package Utils
**Fungsi**: Utilities untuk testing dan monitoring  
**Fitur Utama**:
- Test utilities
- Logging terstruktur
- Metrics collection

## 🔧 Konfigurasi

### Konfigurasi Client
```go
config := &client.Config{
    HostPort:     "localhost:7233",          // Alamat Temporal server
    Namespace:    "default",                 // Namespace yang digunakan
    Identity:     "aplikasi-saya-v1.0",     // Identitas client
    TLS:          nil,                       // Konfigurasi TLS (opsional)
}
```

### Konfigurasi Worker
```go
options := &worker.Options{
    MaxConcurrentWorkflowTasks: 100,    // Maksimal workflow bersamaan
    MaxConcurrentActivityTasks: 200,    // Maksimal activity bersamaan
    WorkflowPollerCount:       2,       // Jumlah poller workflow
    ActivityPollerCount:       4,       // Jumlah poller activity
}
```

## 📝 Contoh Lengkap

Lihat folder `examples/` untuk contoh lengkap:

- **`examples/complete/`**: Demo lengkap semua fitur
- **`examples/cron/`**: Berbagai pola cron jobs
- **`examples/oneshot/`**: One-shot execution patterns  
- **`examples/retry/`**: Strategi retry yang berbeda

### Menjalankan Contoh

```bash
# 1. Jalankan Temporal server dulu
docker run --rm -p 7233:7233 -p 8233:8233 temporalio/auto-setup:latest

# 2. Di terminal lain, jalankan contoh
cd examples/complete
go run main.go

# Atau contoh lainnya
cd examples/cron
go run main.go
```

## 🧪 Testing

```go
// Test dasar module
go run test-basic.go

// Test dengan build
go build ./...

// Test individual package
go test ./client -v
go test ./worker -v
```

## 🚀 Deploy ke Production

### Dengan Docker
```dockerfile
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o app main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/app .
CMD ["./app"]
```

### Environment Variables
```bash
# Konfigurasi Temporal
TEMPORAL_HOST=localhost:7233
TEMPORAL_NAMESPACE=default

# Konfigurasi aplikasi
APP_NAME=aplikasi-saya
LOG_LEVEL=info
```

## ❓ Troubleshooting

### Masalah Umum

1. **Tidak bisa connect ke Temporal**
   ```bash
   # Pastikan Temporal server jalan
   docker ps | grep temporal
   
   # Test koneksi
   telnet localhost 7233
   ```

2. **Worker tidak receive task**
   ```go
   // Pastikan TaskQueue sama antara client dan worker
   TaskQueue: "nama-queue-yang-sama"
   ```

3. **Workflow tidak terdaftar**
   ```go
   // Daftarkan workflow sebelum start worker
   worker.RegisterWorkflow(NamaWorkflow)
   ```

### Debug Mode
```go
// Enable logging detail
config := &client.Config{
    HostPort: "localhost:7233",
    Debug:    true,  // Enable debug mode
}
```

## 🤝 Kontribusi

1. Fork repository ini
2. Buat feature branch (`git checkout -b fitur/fitur-keren`)
3. Commit perubahan (`git commit -m 'Tambah fitur keren'`)
4. Push ke branch (`git push origin fitur/fitur-keren`)
5. Buat Pull Request

## 📄 Lisensi

Project ini menggunakan lisensi MIT - lihat file [LICENSE](LICENSE) untuk detail.

## 🆘 Bantuan

- **Issues**: [GitHub Issues](https://github.com/kangkosw/go-mod-temporal/issues)
- **Dokumentasi**: Folder [examples/](examples/) untuk contoh praktis
- **Community**: [Temporal Community](https://community.temporal.io/)

## 🙏 Credits

- [Temporal.io](https://temporal.io/) untuk workflow engine yang luar biasa
- [Go Team](https://golang.org/) untuk bahasa Go yang awesome
- Komunitas Go Indonesia untuk inspirasi dan dukungan

---

**Dibuat dengan ❤️ untuk komunitas developer Indonesia**

*"Workflow yang mudah, aplikasi yang robust!"*