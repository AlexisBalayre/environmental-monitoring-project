# Cloud Computing Project

## Project Overview

This project demonstrates the integration of cloud computing with the Internet of Things (IoT) for real-time environmental monitoring, focusing on air quality. The aim is to efficiently manage large volumes of data from IoT sensors and accurately calculate the Air Quality Index (AQI).

## System Architecture

- **Data Collection**: IoT sensors collect environmental data.
- **Data Processing**: Utilises Apache Spark for data processing.
- **Data Storage**: Employs Amazon Timestream as the time-series database.
- **Data Visualisation**: Data is visualised through a Grafana dashboard.

## Installation and Setup

### Prerequisites

- Python 3
- Apache Spark
- Amazon Timestream
- Grafana

### Installation Steps

1. **Create a Virtual Environment**:
   ```shell
   python3 -m venv venv
   ```
2. **Activate the Virtual Environment**:
   ```shell
   source venv/bin/activate
   ```
3. **Install Dependencies**:
   ```shell
   pip3 install -r requirements.txt
   ```
4. **Run the Main Program**:
   ```shell
   python3 main.py
   ```

## Usage

After installation, run `python3 main.py` to start the system. Access the Grafana dashboard for data visualisation and analysis.

## Challenges and Solutions

The report discusses various technical challenges and innovative solutions implemented in this project.

## Environmental Impact

This integration significantly contributes to enhanced environmental monitoring and data analysis capabilities.
