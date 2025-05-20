# Cassandra Query Interface

A React-based web interface for executing Cassandra queries using either CQLSH or Java driver.

## Features

- Connect to Cassandra using CQLSH or Java driver
- Execute CQL queries
- View query results in a tabular format
- Support for optional keyspace selection
- Error handling and loading states

## Prerequisites

- Node.js (v14 or higher)
- npm or yarn
- Access to a Cassandra database

## Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```
   or
   ```bash
   yarn install
   ```

3. Start the development server:
   ```bash
   npm start
   ```
   or
   ```bash
   yarn start
   ```

4. Open [http://localhost:3000](http://localhost:3000) in your browser

## Usage

1. Choose your preferred driver (CQLSH or Java)
2. Enter your Cassandra connection details:
   - Host
   - Port (default: 9042)
   - Username
   - Password
   - Keyspace (optional)
3. Click "Connect"
4. Once connected, you can:
   - Enter CQL queries in the query editor
   - Execute queries and view results
   - Disconnect when done

## Note

This is a frontend-only implementation. You'll need to implement the backend API endpoint `/api/execute-query` to handle the actual query execution using the selected driver. 