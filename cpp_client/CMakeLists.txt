cmake_minimum_required(VERSION 3.18)
project(PayrollClientCpp)

# Menggunakan C++ versi terbaru (C++ 17)
set(CMAKE_CXX_STANDARD 17)

# Mencari library Arrow di komputer (Ini bagian tersulitnya)
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)

# Membuat file executeable bernama "client_app"
add_executable(client_app client.cpp)

# Menghubungkan library Arrow ke aplikasi kita
target_link_libraries(client_app PRIVATE Arrow::arrow_shared ArrowFlight::arrow_flight_shared)