"""
Airflow Learning Script - Windows Compatible
==========================================

This script demonstrates Airflow concepts without requiring a full Airflow installation.
You can run this with regular Python to understand DAG structure and task logic.
"""

from datetime import datetime, timedelta
import time

class MockTask:
    """Mock Airflow task for learning purposes"""
    
    def __init__(self, task_id, task_type="python", command=None, python_callable=None):
        self.task_id = task_id
        self.task_type = task_type
        self.command = command
        self.python_callable = python_callable
        self.upstream_tasks = []
        self.downstream_tasks = []
    
    def set_upstream(self, task):
        """Set upstream dependency"""
        if task not in self.upstream_tasks:
            self.upstream_tasks.append(task)
        if self not in task.downstream_tasks:
            task.downstream_tasks.append(self)
    
    def set_downstream(self, task):
        """Set downstream dependency"""
        if task not in self.downstream_tasks:
            self.downstream_tasks.append(task)
        if self not in task.upstream_tasks:
            task.upstream_tasks.append(self)
    
    def __rshift__(self, other):
        """Enable >> syntax for dependencies"""
        if isinstance(other, list):
            for task in other:
                self.set_downstream(task)
        else:
            self.set_downstream(other)
        return other
    
    def run(self):
        """Execute the task"""
        print(f"🚀 Executing task: {self.task_id}")
        
        if self.task_type == "python" and self.python_callable:
            result = self.python_callable()
            print(f"✅ Task {self.task_id} completed. Result: {result}")
        elif self.task_type == "bash" and self.command:
            print(f"💻 Running bash command: {self.command}")
            print(f"✅ Task {self.task_id} completed")
        else:
            print(f"✅ Task {self.task_id} completed (dummy task)")
        
        time.sleep(1)  # Simulate processing time

class MockDAG:
    """Mock Airflow DAG for learning purposes"""
    
    def __init__(self, dag_id, description="", schedule_interval="@daily"):
        self.dag_id = dag_id
        self.description = description
        self.schedule_interval = schedule_interval
        self.tasks = []
    
    def add_task(self, task):
        """Add task to DAG"""
        if task not in self.tasks:
            self.tasks.append(task)
    
    def get_task_order(self):
        """Get tasks in execution order (topological sort)"""
        # Simple topological sort for demo
        visited = set()
        order = []
        
        def visit(task):
            if task in visited:
                return
            visited.add(task)
            
            for upstream in task.upstream_tasks:
                visit(upstream)
            
            order.append(task)
        
        for task in self.tasks:
            visit(task)
        
        return order
    
    def run(self):
        """Execute the DAG"""
        print(f"🎯 Starting DAG: {self.dag_id}")
        print(f"📝 Description: {self.description}")
        print("=" * 50)
        
        execution_order = self.get_task_order()
        
        for task in execution_order:
            task.run()
        
        print("=" * 50)
        print(f"🎉 DAG {self.dag_id} completed successfully!")

# Example functions for tasks
def extract_data():
    """Simulate data extraction"""
    print("📥 Extracting data from source...")
    data = {"users": 100, "orders": 250, "revenue": 15000}
    return data

def transform_data():
    """Simulate data transformation"""
    print("🔄 Transforming data...")
    # Simulate some processing
    result = {"avg_order_value": 60, "user_conversion": 0.25}
    return result

def load_data():
    """Simulate data loading"""
    print("📤 Loading data to warehouse...")
    return "Data loaded successfully"

def send_notification():
    """Simulate sending notification"""
    print("📧 Sending completion notification...")
    return "Notification sent"

# Create a sample DAG
def create_sample_dag():
    """Create a sample ETL DAG"""
    
    # Create DAG
    dag = MockDAG(
        dag_id="sample_etl_dag",
        description="Sample ETL pipeline for learning",
        schedule_interval="@daily"
    )
    
    # Create tasks
    start_task = MockTask("start", "dummy")
    
    extract_task = MockTask(
        "extract_data", 
        "python", 
        python_callable=extract_data
    )
    
    transform_task = MockTask(
        "transform_data", 
        "python", 
        python_callable=transform_data
    )
    
    load_task = MockTask(
        "load_data", 
        "python", 
        python_callable=load_data
    )
    
    validate_task = MockTask(
        "validate_data", 
        "bash", 
        command="echo 'Data validation completed'"
    )
    
    notify_task = MockTask(
        "send_notification", 
        "python", 
        python_callable=send_notification
    )
    
    end_task = MockTask("end", "dummy")
    
    # Add tasks to DAG
    for task in [start_task, extract_task, transform_task, load_task, validate_task, notify_task, end_task]:
        dag.add_task(task)
    
    # Define dependencies using >> syntax (just like real Airflow!)
    start_task >> extract_task >> transform_task >> load_task >> validate_task >> notify_task >> end_task
    
    return dag

def create_parallel_dag():
    """Create a DAG with parallel tasks"""
    
    dag = MockDAG(
        dag_id="parallel_processing_dag",
        description="DAG demonstrating parallel task execution",
        schedule_interval="@hourly"
    )
    
    start = MockTask("start", "dummy")
    
    # Parallel processing tasks
    process_users = MockTask("process_users", "python", 
                           python_callable=lambda: print("Processing user data..."))
    
    process_orders = MockTask("process_orders", "python", 
                            python_callable=lambda: print("Processing order data..."))
    
    process_inventory = MockTask("process_inventory", "python", 
                               python_callable=lambda: print("Processing inventory data..."))
    
    # Combine results
    combine_results = MockTask("combine_results", "python", 
                             python_callable=lambda: print("Combining all results..."))
    
    end = MockTask("end", "dummy")
    
    # Add to DAG
    for task in [start, process_users, process_orders, process_inventory, combine_results, end]:
        dag.add_task(task)
    
    # Parallel execution pattern
    start >> [process_users, process_orders, process_inventory] >> combine_results >> end
    
    return dag

if __name__ == "__main__":
    print("🎓 Welcome to Airflow Learning!")
    print("This script demonstrates Airflow concepts without requiring full installation.\n")
    
    # Run the sample ETL DAG
    print("1️⃣ Running Sample ETL DAG:")
    etl_dag = create_sample_dag()
    etl_dag.run()
    
    print("\n" + "="*60 + "\n")
    
    # Run the parallel processing DAG
    print("2️⃣ Running Parallel Processing DAG:")
    parallel_dag = create_parallel_dag()
    parallel_dag.run()
    
    print("\n🎯 Key Concepts Learned:")
    print("✅ DAG structure and creation")
    print("✅ Task dependencies using >> syntax")
    print("✅ Sequential and parallel task execution")
    print("✅ Different task types (Python, Bash, Dummy)")
    print("✅ Task execution order (topological sort)")
    
    print("\n📚 Next Steps:")
    print("• Try GitHub Codespaces for full Airflow experience")
    print("• Upload the Jupyter notebook to Google Colab")
    print("• Practice creating your own DAG structures")
    print("• Learn about Airflow operators, sensors, and hooks")
