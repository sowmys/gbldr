from neo4j import GraphDatabase
import uuid
from faker import Faker
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

from neo4j import GraphDatabase
import uuid
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Fixed set of Admin Units
ADMIN_UNITS = [
    "North America", "Europe", "Asia", "Australia", "Africa",
    "US Gov", "South America", "Europe Trade Union"
]

# Fixed list of Sensitivity Types
SENSITIVITY_TYPES = ["PII", "PHI", "Financial"]

# Initialize Faker
fake = Faker()

# Function to generate random datetime within a given range
def random_date(start_date=None, end_date=None):
    if start_date is None:
        start_date = datetime.now() - timedelta(days=365)
    if end_date is None:
        end_date = datetime.now()
    # Generate a random date within the range
    random_date = fake.date_between(start_date=start_date, end_date=end_date)
    # Generate a random time for the selected date
    random_time = fake.time_object()
    # Combine the date and time into a datetime object
    return datetime.combine(random_date, random_time)

# Function to generate exponential random integer for operation count
def random_exponential_int(scale=10000):
    return max(1, int(random.expovariate(1 / scale)))

# Generate a GUID
def generate_guid():
    return str(uuid.uuid4())

# Generate sample data
def create_nodes():
    return {
        "ENTRAGROUP": [{
            "objectId": generate_guid(),
            "adminUnit": random.choice(ADMIN_UNITS),
            "groupName": fake.company(),
            "email": fake.company_email()
        } for _ in range(100)],
        "ENTRAUSER": [{
            "objectId": generate_guid(),
            "adminUnit": random.choice(ADMIN_UNITS),
            "userPrincipalName": fake.email(),
            "DisplayName": fake.name(),
            "email": fake.email()
        } for _ in range(3000)],
        "ENTRAROLE": [{
            "objectId": generate_guid(),
            "adminUnit": random.choice(ADMIN_UNITS),
            "rolename": fake.job(),
            "description": fake.text(max_nb_chars=50)
        } for _ in range(10)],
        "SERVICEPRINCIPAL": [{
            "objectId": generate_guid(),
            "adminUnit": random.choice(ADMIN_UNITS),
            "appOwnerOrganizationID": generate_guid()
        } for _ in range(100)],
        "MANAGEDIDENTITY": [{
            "objectId": generate_guid(),
            "adminUnit": random.choice(ADMIN_UNITS)
        } for _ in range(100)],
        "APPLICATION": [{
            "objectId": generate_guid(),
            "adminUnit": random.choice(ADMIN_UNITS),
            "applicationID": generate_guid(),
            "name": fake.catch_phrase()
        } for _ in range(100)],
        "ONLINEFILE": [{
            "objectId": generate_guid(),
            "adminUnit": random.choice(ADMIN_UNITS),
            "name": fake.file_name(),
            "ItemType": random.choice(["File", "Document", "Image"]),
            "workload": random.choice(["SharePoint", "Exchange", "Teams"]),
            "IsViewableByExternalUsers": random.choice(["Yes", "No"]),
            "sensitivityLabelId": generate_guid()
        } for _ in range(12000)],
        "SITE": [{
            "objectId": generate_guid(),
            "adminUnit": random.choice(ADMIN_UNITS),
            "siteUrl": fake.url()
        } for _ in range(500)],
        # One SENSITIVEINFOTYPE node per combination of Sensitivity Type and Admin Unit
        "SENSITIVEINFOTYPE": [{
            "objectId": generate_guid(),
            "adminUnit": admin_unit,
            "sensitveInfoType": sensitivity_type
        } for admin_unit in ADMIN_UNITS for sensitivity_type in SENSITIVITY_TYPES],
    }

# Upload data to Neo4j
def upload_nodes(session):
    data = create_nodes()
    # Upload nodes
    for label, nodes in data.items():
        for node in nodes:
            properties = ", ".join([f"{key}: ${key}" for key in node.keys()])
            session.run(f"CREATE (:{label} {{ {properties} }})", node)

# Function to create all relationships
def upload_edges(session):
    # (ENTRAUSER) - [:PerformedOperation] -> (ONLINEFILE)
    # Updated Cypher query
    session.run("""
        MATCH (u:ENTRAUSER), (f:ONLINEFILE)
        WHERE u.adminUnit = f.adminUnit
        WITH u, f, rand() AS r
        WHERE r < 0.01  // Only about 10 people touch a file out of 1000 users
        CREATE (u)-[:PerformedOperation {
            OperationType: $operationType, 
            OperationCount: $operationCount, 
            EarliestOperationTime: $earliestOperationTime, 
            LatestOperationTime: $latestOperationTime
        }]->(f)
    """, {
        "operationType": random.choice(["Edit", "Upload", "Download"]),
        "operationCount": random_exponential_int(100),
        "earliestOperationTime": (earliest := random_date()).isoformat(),
        "latestOperationTime": (earliest + timedelta(seconds=random.randint(1, 86400))).isoformat()
    })

    # (ONLINEFILE) - [:CreatedBy] -> (ENTRAUSER)
    session.run("""
        MATCH (f:ONLINEFILE), (u:ENTRAUSER)
        WHERE f.adminUnit = u.adminUnit
        AND NOT EXISTS((f)-[:CreatedBy]->())
        WITH f, COLLECT(u) AS users
        WITH f, users[toInteger(rand() * SIZE(users))] AS randomUser
        MERGE (f)-[:CreatedBy {created_on: $created_on}]->(randomUser)
    """, {"created_on": random_date()})

    # (SENSITIVEINFOTYPE) - [:SensitivityLabelOwnedBy] -> (ENTRAUSER)
    session.run("""
        MATCH (f:SENSITIVEINFOTYPE), (u:ENTRAUSER)
        WHERE f.adminUnit = u.adminUnit
        AND NOT EXISTS((f)-[:SensitivityLabelOwnedBy]->())
        WITH f, COLLECT(u) AS users
        WITH f, users[toInteger(rand() * SIZE(users))] AS randomUser
        MERGE (f)-[:SensitivityLabelOwnedBy {created_on: $created_on}]->(randomUser)
    """, {"created_on": random_date()})

    # (ONLINEFILE) - [:ContainedBy] -> (SITE)
    session.run("""
        MATCH (f:ONLINEFILE), (s:SITE)
        WHERE f.adminUnit = s.adminUnit
        AND NOT EXISTS((f)-[:ContainedBy]->())
        WITH f, COLLECT(s) AS sites
        WITH f, sites[toInteger(rand() * SIZE(sites))] AS randomSite
        MERGE (f)-[:ContainedBy {created_on: $created_on}]->(randomSite)
    """, {"created_on": random_date()})

    # (ONLINEFILE) - [:OwnedBy] -> (ENTRAUSER)
    session.run("""
        MATCH (f:ONLINEFILE), (u:ENTRAUSER)
        WHERE f.adminUnit = u.adminUnit
        AND NOT EXISTS((f)-[:OwnedBy]->())
        WITH f, COLLECT(u) AS users
        WITH f, users[toInteger(rand() * SIZE(users))] AS randomUser
        MERGE (f)-[:OwnedBy {created_on: $created_on}]->(randomUser)
    """, {"created_on": random_date()})

    # (ONLINEFILE) - [:IsOfSensitiveType] -> (SENSITIVEINFOTYPE)
    session.run("""
        MATCH (f:ONLINEFILE), (s:SENSITIVEINFOTYPE)
        WHERE f.adminUnit = s.adminUnit
        WITH f, s, rand() AS r
        WHERE r < 0.2  // Only 20% file have SENSITIVEINFOTYPE
        CREATE (f)-[:IsOfSensitiveType {created_on: $created_on}]->(s)
    """, {"created_on": random_date()})

    # (SERVICEPRINCIPAL) - [:MemberOf] -> (ENTRAGROUP)
    session.run("""
        MATCH (sp:SERVICEPRINCIPAL), (g:ENTRAGROUP)
        WHERE sp.adminUnit = g.adminUnit
        WITH sp, g, rand() AS r
        WHERE r < 0.1  
        CREATE (sp)-[:MemberOf {since: $since}]->(g)
    """, {"since": random_date()})

    # (ENTRAGROUP) - [:MemberOf] -> (ENTRAGROUP)
    session.run("""
        MATCH (g1:ENTRAGROUP), (g2:ENTRAGROUP)
        WHERE g1 <> g2 AND g1.adminUnit = g2.adminUnit
        WITH g1, g2, rand() AS r
        WHERE r < 0.1  
        CREATE (g1)-[:MemberOf {since: $since}]->(g2)
    """, {"since": random_date()})

    # (ENTRAUSER) - [:MemberOf] -> (ENTRAGROUP)
    session.run("""
        MATCH (u:ENTRAUSER), (g:ENTRAGROUP)
        WHERE u.adminUnit = g.adminUnit
        WITH u, g, rand() AS r
        WHERE r < 0.6  // 60% chance
        CREATE (u)-[:MemberOf {since: $since}]->(g)
    """, {"since": random_date()})

    # (MANAGEDIDENTITY) - [:MemberOf] -> (ENTRAGROUP)
    session.run("""
        MATCH (mi:MANAGEDIDENTITY), (g:ENTRAGROUP)
        WHERE mi.adminUnit = g.adminUnit
        WITH mi, g, rand() AS r
        WHERE r < 0.5  // 50% chance
        CREATE (mi)-[:MemberOf {since: $since}]->(g)
    """, {"since": random_date()})

    # (APPLICATION) - [:CanAuthenticateAs] -> (MANAGEDIDENTITY)
    session.run("""
        MATCH (a:APPLICATION), (mi:MANAGEDIDENTITY)
        WHERE a.adminUnit = mi.adminUnit
        WITH a, mi, rand() AS r
        WHERE r < 0.4  // 40% chance
        CREATE (a)-[:CanAuthenticateAs {created_on: $created_on, since: $since, lastUpdated_on: $last_updated}]->(mi)
    """, {"created_on": random_date(), "since": random_date(), "last_updated": random_date()})

    # (APPLICATION) - [:CanAuthenticateAs] -> (SERVICEPRINCIPAL)
    session.run("""
        MATCH (a:APPLICATION), (sp:SERVICEPRINCIPAL)
        WHERE a.adminUnit = sp.adminUnit
        WITH a, sp, rand() AS r
        WHERE r < 0.4  // 40% chance
        CREATE (a)-[:CanAuthenticateAs {created_on: $created_on, since: $since, lastUpdated_on: $last_updated}]->(sp)
    """, {"created_on": random_date(), "since": random_date(), "last_updated": random_date()})

    # (ENTRAUSER) - [:HasRole] -> (ENTRAGROUP)
    session.run("""
        MATCH (u:ENTRAUSER), (g:ENTRAGROUP)
        WHERE u.adminUnit = g.adminUnit
        WITH u, g, rand() AS r
        WHERE r < 0.5  // 50% chance
        CREATE (u)-[:HasRole {created_on: $created_on, since: $since, lastUpdated_on: $last_updated}]->(g)
    """, {"created_on": random_date(), "since": random_date(), "last_updated": random_date()})

    # (SERVICEPRINCIPAL) - [:HasRole] -> (ENTRAGROUP)
    session.run("""
        MATCH (sp:SERVICEPRINCIPAL), (g:ENTRAGROUP)
        WHERE sp.adminUnit = g.adminUnit
        WITH sp, g, rand() AS r
        WHERE r < 0.5  // 50% chance
        CREATE (sp)-[:HasRole {created_on: $created_on, since: $since, lastUpdated_on: $last_updated}]->(g)
    """, {"created_on": random_date(), "since": random_date(), "last_updated": random_date()})

# Main execution
if __name__ == "__main__":
    load_dotenv()
    NEO4J_URI = os.getenv("NEO4J_URI_DESKTOP")
    NEO4J_USER = os.getenv("NEO4J_USER")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD_DESKTOP")

    # NEO4J_URI = os.getenv("NEO4J_URI")
    # NEO4J_USER = os.getenv("NEO4J_USER")
    # NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    with driver.session() as session:
        upload_nodes(session)
        upload_edges(session)

    print("Data uploaded successfully.")



