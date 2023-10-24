import boto3

class CloudFormationManager:

    """
    A class for managing AWS CloudFormation stacks using Boto3.

    This class provides methods to create, update, and delete CloudFormation stacks.

    Args:
        aws_access_key_id (str): AWS access key ID.
        aws_secret_access_key (str): AWS secret access key.
        region_name (str): AWS region name.

    Attributes:
        session: Boto3 session for AWS.
        cf_client: CloudFormation client for AWS.

    """

    def __init__(self, aws_access_key_id: str, aws_secret_access_key:str, region_name: str):

        """
        Initialize a CloudFormationManager instance.

        Args:
            aws_access_key_id (str): AWS access key ID.
            aws_secret_access_key (str): AWS secret access key.
            region_name (str): AWS region name.
        """
        
        # Initialize a session with AWS credentials and region
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        # Create a CloudFormation client
        self.cf_client = self.session.client('cloudformation')



    def create_stack(self, stack_name: str, template_file_path: str):

        """
        Create a new AWS CloudFormation stack from local file.

        Args:
            stack_name (str): Name of the CloudFormation stack.
            template_file_path (str): File path to the CloudFormation template.
        """

        # Read the CloudFormation template from the file
        with open(template_file_path, 'r') as template_file:
            template_body = template_file.read()
        
        try:
            # Create a new CloudFormation stack
            self.cf_client.create_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Capabilities=['CAPABILITY_NAMED_IAM'],
            )
            # Wait for the stack creation to complete
            self.cf_client.get_waiter('stack_create_complete').wait(StackName=stack_name)
            print(f'Successfully created CloudFormation stack: {stack_name}')
        except Exception as e:
            print(f'An error occurred: {str(e)}')



    def update_stack(self, stack_name: str, template_file_path: str):

        """
        Update an existing AWS CloudFormation stack from local file.

        Args:
            stack_name (str): Name of the CloudFormation stack to update.
            template_file_path (str): File path to the updated CloudFormation template.
        """

        # Read the updated CloudFormation template from the file
        with open(template_file_path, 'r') as template_file:
            template_body = template_file.read()

        try:
            # Update an existing CloudFormation stack
            self.cf_client.update_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Capabilities=['CAPABILITY_NAMED_IAM'],
            )
            # Wait for the stack update to complete
            self.cf_client.get_waiter('stack_update_complete').wait(StackName=stack_name)
            print(f'Successfully updated CloudFormation stack: {stack_name}')
        except Exception as e:
            print(f'An error occurred: {str(e)}')



    def delete_stack(self, stack_name: str):

        """
        Delete an existing AWS CloudFormation stack.

        Args:
            stack_name (str): Name of the CloudFormation stack to delete.
        """

        try:
            # Delete a CloudFormation stack
            self.cf_client.delete_stack(StackName=stack_name)
            # Wait for the stack deletion to complete
            self.cf_client.get_waiter('stack_delete_complete').wait(StackName=stack_name)
            print(f'Successfully deleted CloudFormation stack: {stack_name}')
        except Exception as e:
            print(f'An error occurred: {str(e)}')


# Example usage:
stack_name = 'test-stack'
template_file_path = 'cloudformation-template.yaml'
aws_access_key_id='AWS_ACCESS_KEY_ID'
aws_secret_access_key='AWS_SECRET_ACCESS_KEY'
region_name='REGION'

# Create an instance of the CloudFormationManager class
cf_manager = CloudFormationManager(aws_access_key_id, aws_secret_access_key, region_name)

# Delete an existing stack
cf_manager.create_stack(stack_name, 'cloudformation-template.yaml')

# # Delete an existing stack
# cf_manager.update_stack(stack_name, 'cloudformation-template.yaml')

# # Delete an existing stack
# cf_manager.delete_stack(stack_name)