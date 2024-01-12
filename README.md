# Cloud Computing and IoT for Environmental Monitoring

## Overview 🌍

This project integrates Cloud Computing with the Internet of Things (IoT) for comprehensive environmental monitoring, with a focus on air quality analysis. Utilising Apache Spark and Amazon Timestream, it manages large volumes of data from IoT sensors to calculate the Air Quality Index (AQI) accurately, offering insights into environmental conditions. The file `report.pdf` provides a detailed overview of the project.

## Key Features 🌟

- **Real-time Data Collection**: IoT sensors gather environmental data continuously.
- **Efficient Data Processing**: Leverages Apache Spark for effective data handling.
- **Robust Data Storage**: Uses Amazon Timestream for optimised time-series data management.
- **Dynamic Data Visualisation**: Features a Grafana dashboard for interactive and real-time data insights.

## Pipeline Architecture 🏗️
<img width="795" alt="pipeline" src="https://github.com/AlexisBalayre/environmental-monitoring-project/assets/60859013/3391584a-9027-4075-9ba1-9294eb4cf58e">

## Grafana Dashboard 📊
<img width="1512" alt="grafana-1" src="https://github.com/AlexisBalayre/environmental-monitoring-project/assets/60859013/daefbf54-d1cd-42ac-acbd-529b16b2b9ee">
<img width="1512" alt="grafana-2" src="https://github.com/AlexisBalayre/environmental-monitoring-project/assets/60859013/9017a1b5-72f3-4c46-9f89-f4d8a954dcee">
<img width="1512" alt="grafana-4" src="https://github.com/AlexisBalayre/environmental-monitoring-project/assets/60859013/cd2ed19b-1109-409c-a69c-694f42099fac">

## Getting Started 🚀

### Installation

1. **Clone the repository**:

   ```bash
   git clone git@github.com:AlexisBalayre/environmental-monitoring-project.git
   ```

2. **Navigate to the project directory**:

   ```bash
   cd environmental-monitoring-project
   ```

3. **Set up a virtual environment**:

   ```bash
   python3 -m venv venv
   ```

4. **Activate the virtual environment**:

   - For Windows:

     ```bash
     .\venv\Scripts\activate
     ```

   - For Unix or MacOS:

     ```bash
     source venv/bin/activate
     ```

5. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

### Usage

- **Start the system**: Run `main.py` to begin data collection and processing.

  ```bash
  python main.py
  ```

- **Visualise the data**: Access the Grafana dashboard for real-time data analysis and visualisations.

## Project Structure 📂

- `lib/`: Core library modules for data collection, processing, and storage.
- `scripts/`: Scripts for IAM credentials retrieval and Spark job initiation.
- `services/`: Service configurations for IAM and Spark.
- `test/`: Testing scripts and visualisation tools.
- `main.py`: Main executable script.
- `requirements.txt`: Project dependencies.

## Testing 🧪

The project includes comprehensive testing:

- Load testing configurations and results.
- Unit testing for data collection, processing, and storage.
- Visualisation tools for data analysis.

## Dependencies 🛠️

- Apache Spark
- Python 3.x
- Amazon Timestream
- Grafana
