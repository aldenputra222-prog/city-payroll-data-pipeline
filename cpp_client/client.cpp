#include <iostream>
#include <memory>
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>

// Namespace biar gak ngetik panjang-panjang
namespace flight = arrow::flight;

int main() {
    // 1. Inisialisasi Koneksi ke Server Python (Localhost:8815)
    std::string host = "grpc://0.0.0.0:8815";
    std::cout << "Connecting to " << host << "..." << std::endl;

    // Location object
    arrow::Result<flight::Location> location_result = flight::Location::Parse(host);
    if (!location_result.ok()) {
        std::cerr << "Error parsing location: " << location_result.status().ToString() << std::endl;
        return -1;
    }
    flight::Location location = *location_result;

    // Client object
    std::unique_ptr<flight::FlightClient> client;
    arrow::Result<std::unique_ptr<flight::FlightClient>> client_result = flight::FlightClient::Connect(location);
    if (!client_result.ok()) {
        std::cerr << "Could not connect: " << client_result.status().ToString() << std::endl;
        return -1;
    }
    client = std::move(*client_result);

    // 2. Siapkan Tiket (JSON Command)
    // Di C++, JSON hanyalah string biasa kalau kita tidak pakai library JSON tambahan
    std::string json_command = "{\"action\": \"get_all\"}"; 
    flight::Ticket ticket{json_command};

    // 3. Request Data (DoGet)
    std::unique_ptr<flight::FlightStreamReader> reader;
    auto result = client->DoGet(ticket);
    if (!result.ok()) {
        std::cerr << "Request failed: " << result.status().ToString() << std::endl;
        return -1;
    }
    reader = std::move(*result);

    // 4. Baca Stream Data
    std::shared_ptr<arrow::Table> table;
    arrow::Status status = reader->ReadAll(&table); // Baca semua ke memori

    if (!status.ok()) {
        std::cerr << "Error reading stream: " << status.ToString() << std::endl;
        return -1;
    }

    // 5. Tampilkan Hasil (Bukti Sukses)
    std::cout << "------------------------------------------------" << std::endl;
    std::cout << "SUCCESS! Received data via C++ Client" << std::endl;
    std::cout << "Total Rows: " << table->num_rows() << std::endl;
    std::cout << "Total Columns: " << table->num_columns() << std::endl;
    std::cout << "------------------------------------------------" << std::endl;
    
    // Print skema kolom untuk memastikan kita terima 'job_title'
    std::cout << "Schema:" << std::endl;
    std::cout << table->schema()->ToString() << std::endl;

    return 0;
}