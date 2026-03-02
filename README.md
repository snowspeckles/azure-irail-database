# Belgian Train Data Pipeline

A real-world data pipeline that fetches real-time train departure data from the iRail API, normalises it, and stores it in a SQL database using Microsoft Azure. The visualisation is done using cloud-native dashboard that provides insights into train operations in Belgium. This project demonstrates a complete cloud-native data solution with progressive complexity levels.

## Project Structure

The project is structured in three progressive levels:

- 🟢 **Must-Have**: Core functionality - fetch and store data via Azure Portal using Azure Functions and Azure SQL Database
- 🟡 **Nice-to-Have**: Add automation (scheduling), build a live dashboard (Power BI), and enable data refresh
- 🔴 **Hardcore Level**: Full DevOps integration - CI/CD pipelines, Azure CLI, Docker deployment, and infrastructure as code

## Technologies & Tools Used

### Core Azure Services
| Service | Purpose |
|---------|---------|
| **Azure Function App (Python 3.11)** | Serverless data ingestion logic |
| **Azure SQL Database** | Storage for normalised train data |
| **Azure Storage Account** | Dependency for Function App |
| **App Service Plan (Consumption)** | Hosts Functions with autoscaling |

### Data Processing & Storage
| Tool/Technology | Purpose |
|-----------------|---------|
| **Python 3.11** | Primary programming language for Azure Functions |
| **pandas** | JSON normalization and data manipulation |
| **iRail API** | Source of real-time Belgian train data from the following endpoints: `/liveboard`, `/connections` |
| **Azure SQL Database** | Structured data storage with proper SQL data types |

### Automation & Monitoring
| Tool/Service | Purpose |
|--------------|---------|
| **HTTP Trigger Functions** | HTTP-triggered Azure Functions for on-demand data retrieval |
| **Timer Trigger Functions** | Scheduled data fetching (hourly intervals) |
| **Application Insights** | Runtime metrics and delay tracking |
| **Azure Portal** | Infrastructure management and deployment |

### Advanced DevOps (Hardcore Level)
| Tool/Service | Purpose |
|--------------|---------|
| **GitHub Actions / Azure DevOps** | CI/CD pipeline automation |
| **Terraform** | Infrastructure as Code (IaC) |
| **Azure CLI** | Script-based resource management |
| **Docker** | Containerisation for Functions |
| **Azure Container Registry** | Container image storage |
| **Managed Identities** | Secure authentication without hardcoded secrets |

### Data Visualization
| Tool/Service | Purpose |
|--------------|---------|
| **Power BI Service** | Live dashboard showing train routes, connections, platform information, and train types |
| **Azure SQL Connector** | Direct database integration |

## Features

### Dashboard Capabilities
- Live departure boards for selected stations
- Delay monitoring and analysis
- Route exploration between cities
- Train type distribution visualization
- Peak hour analysis
- Real-time train mapping (advanced)

## Project Structure

```
├── azure-functions/              # Azure Function code
│   ├── function_app.py           # Main function logic
│   ├── requirements.txt          # Python dependencies
│   └── host.json                 # Function configuration
├── terraform/                    # Infrastructure as Code (Hardcore Level)
│   ├── main.tf                   # Azure resource definitions
│   └── variables.tf              # Configuration variables
├── scripts/                      # Automation scripts
│   ├── deploy.sh                 # Deployment automation
│   └── setup_db.sql              # Database initialization
├── power-bi/                     # Power BI dashboard files
│   ├── train_dashboard.pbix
│   └── data_source_config
└── docs/                         # Documentation
    ├── api-endpoints.md
    └── deployment-guide.md
```

## Getting Started

### Prerequisites
- Microsoft Azure account
- Python 3.11 installed locally
- Azure Portal: Azure SQL database
- Git for version control

## Configuration

### Environment Variables
```python
# Azure Function App Settings
DB_CONNECTION_STRING="your_sql_connection_string"
IRAIL_API_BASE_URL="https://api.irail.be"
FUNCTION_NAME="train-data-ingestion"
```

### Database Schema
```sql
CREATE TABLE TrainDepartures (
    id INT PRIMARY KEY IDENTITY(1,1),
    station_name VARCHAR(100),
    train_type VARCHAR(50),
    departure_time DATETIME,
    platform VARCHAR(20),
    delay_minutes INT,
    route_info TEXT,
    vehicle_id VARCHAR(100),
    created_at DATETIME DEFAULT GETDATE()
);
```

## API Endpoints

### iRail API Examples
- `https://api.irail.be/liveboard/?id=BE.NMBS.008811026` - Live departures for a station
- `https://api.irail.be/connections/?from=Brussel-Zuid&to=Antwerpen-Centraal` - Route connections

### Azure Function Endpoints
- `POST /api/train-data` - Trigger manual data fetch
- `GET /api/train-data/status` - Check pipeline health

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   iRail API     │───▶│ Azure Function  │───▶│ Azure SQL DB    │
│   (Data Source) │    │   (Processing)  │    │   (Storage)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Power BI      │◀───│ Application     │◀───│   Timer Trigger │
│   (Dashboard)   │    │ Insights        │    │   (Scheduler)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Security Best Practices

- Use Managed Identities instead of hardcoded secrets
- Implement proper firewall rules for Azure SQL
- Secure Function endpoints with authentication
- Use environment variables for sensitive configuration
- Implement proper error handling without exposing sensitive data

## Deployment

### Azure Portal Deployment
1. Deploy Function App through Azure Portal
2. Configure database connection
3. Test and validate data pipeline
4. Set up monitoring

### CI/CD Pipeline
1. Configure GitHub Actions workflow
2. Set up Terraform infrastructure provisioning
3. Implement automated testing
4. Deploy to production environment
