"""
Integration tests for job CRUD operations.

Tests the databricks_tools_core.jobs functions:
- list_jobs
- find_job_by_name
- create_job
- get_job
- update_job
- delete_job

All tests use serverless compute for job execution.
"""

import logging
import pytest

from databricks_tools_core.jobs import (
    list_jobs,
    find_job_by_name,
    create_job,
    get_job,
    update_job,
    delete_job,
    JobError,
)

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestListJobs:
    """Tests for listing jobs."""

    def test_list_jobs(self):
        """Should list jobs successfully using our core function."""
        logger.info("Testing list_jobs")

        # List jobs with limit
        jobs = list_jobs(limit=10)

        logger.info(f"Found {len(jobs)} jobs")
        for job in jobs[:3]:
            logger.info(f"  - {job.get('name')} (ID: {job.get('job_id')})")

        # Verify response
        assert isinstance(jobs, list)
        # Verify dict structure
        if len(jobs) > 0:
            assert "job_id" in jobs[0]
            assert "name" in jobs[0]

    def test_list_jobs_with_name_filter(self):
        """Should filter jobs by name."""
        logger.info("Testing list_jobs with name filter")

        # This may or may not find jobs depending on workspace
        jobs = list_jobs(name="test", limit=5)

        logger.info(f"Found {len(jobs)} jobs matching 'test'")
        assert isinstance(jobs, list)


@pytest.mark.integration
class TestFindJobByName:
    """Tests for finding jobs by name."""

    def test_find_job_by_name_not_found(self):
        """Should return None when job doesn't exist."""
        non_existent_name = "this_job_definitely_does_not_exist_12345"
        job_id = find_job_by_name(name=non_existent_name)

        logger.info(f"Search for non-existent job '{non_existent_name}': {job_id}")
        assert job_id is None, "Should return None for non-existent job"

    def test_find_job_by_name_exists(
        self,
        test_notebook_path: str,
        cleanup_job,
    ):
        """Should find job when it exists."""
        # Create a test job using our core function (serverless)
        job_name = "test_find_by_name_job"
        tasks = [
            {
                "task_key": "test_task",
                "notebook_task": {
                    "notebook_path": test_notebook_path,
                    "source": "WORKSPACE",
                },
            }
        ]
        job = create_job(name=job_name, tasks=tasks)
        cleanup_job(job["job_id"])

        # Try to find it using our core function
        found_job_id = find_job_by_name(name=job_name)

        logger.info(f"Search for '{job_name}': {found_job_id}")
        assert found_job_id is not None, "Should find the created job"
        assert found_job_id == job["job_id"], "Should return correct job ID"


@pytest.mark.integration
class TestCreateJob:
    """Tests for creating jobs."""

    def test_create_job_basic(
        self,
        test_notebook_path: str,
        cleanup_job,
    ):
        """Should create a simple notebook task job with serverless."""
        job_name = "test_create_job_basic"
        logger.info(f"Creating job: {job_name}")

        # Create job using our core function (serverless - no cluster specified)
        tasks = [
            {
                "task_key": "test_task",
                "notebook_task": {
                    "notebook_path": test_notebook_path,
                    "source": "WORKSPACE",
                },
                "timeout_seconds": 3600,
            }
        ]
        job = create_job(name=job_name, tasks=tasks, max_concurrent_runs=1)

        # Register for cleanup
        cleanup_job(job["job_id"])

        logger.info(f"Job created: {job_name} (ID: {job['job_id']})")

        # Verify creation
        assert job["job_id"] is not None, "Job ID should be set"

    def test_create_job_with_multiple_tasks(
        self,
        test_notebook_path: str,
        cleanup_job,
    ):
        """Should create a job with multiple tasks."""
        job_name = "test_create_job_multi_task"
        logger.info(f"Creating multi-task job: {job_name}")

        # Create job with two tasks using our core function (serverless)
        tasks = [
            {
                "task_key": "task_1",
                "notebook_task": {
                    "notebook_path": test_notebook_path,
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "task_2",
                "depends_on": [{"task_key": "task_1"}],
                "notebook_task": {
                    "notebook_path": test_notebook_path,
                    "source": "WORKSPACE",
                },
            },
        ]
        job = create_job(name=job_name, tasks=tasks, max_concurrent_runs=1)

        # Register for cleanup
        cleanup_job(job["job_id"])

        logger.info(f"Multi-task job created: {job['job_id']}")

        # Verify creation by getting the job
        job_details = get_job(job_id=job["job_id"])
        assert job_details["job_id"] is not None
        assert len(job_details["settings"]["tasks"]) == 2, "Should have two tasks"


@pytest.mark.integration
class TestGetJob:
    """Tests for getting job details."""

    def test_get_job(
        self,
        test_notebook_path: str,
        cleanup_job,
    ):
        """Should get job details by ID."""
        # Create a test job
        job_name = "test_get_job"
        tasks = [
            {
                "task_key": "test_task",
                "notebook_task": {
                    "notebook_path": test_notebook_path,
                    "source": "WORKSPACE",
                },
            }
        ]
        created_job = create_job(name=job_name, tasks=tasks)
        cleanup_job(created_job["job_id"])

        logger.info(f"Getting job details for {created_job['job_id']}")

        # Get job details using our core function
        job = get_job(job_id=created_job["job_id"])

        logger.info(f"Retrieved job: {job['settings']['name']}")

        # Verify details
        assert job["job_id"] == created_job["job_id"], "Job ID should match"
        assert job["settings"]["name"] == job_name, "Job name should match"
        assert job["settings"]["tasks"] is not None, "Should have tasks"

    def test_get_job_not_found(self):
        """Should raise JobError when job doesn't exist."""
        non_existent_id = 999999999

        logger.info(f"Attempting to get non-existent job: {non_existent_id}")

        with pytest.raises(JobError) as exc_info:
            get_job(job_id=non_existent_id)

        logger.info(f"Expected JobError: {exc_info.value}")
        assert exc_info.value.job_id == non_existent_id


@pytest.mark.integration
class TestUpdateJob:
    """Tests for updating jobs."""

    def test_update_job_name(
        self,
        test_notebook_path: str,
        cleanup_job,
    ):
        """Should update job name successfully."""
        # Create a test job
        original_name = "test_update_job_original"
        tasks = [
            {
                "task_key": "test_task",
                "notebook_task": {
                    "notebook_path": test_notebook_path,
                    "source": "WORKSPACE",
                },
            }
        ]
        job = create_job(name=original_name, tasks=tasks)
        cleanup_job(job["job_id"])

        new_name = "test_update_job_renamed"
        logger.info(f"Updating job {job['job_id']} to '{new_name}'")

        # Update job name using our core function
        update_job(job_id=job["job_id"], name=new_name)

        # Verify update
        updated_job = get_job(job_id=job["job_id"])
        logger.info(f"Job updated: {updated_job['settings']['name']}")

        assert updated_job["settings"]["name"] == new_name, "Name should be updated"
        assert updated_job["job_id"] == job["job_id"], "Job ID should stay same"

    def test_update_job_max_concurrent_runs(
        self,
        test_notebook_path: str,
        cleanup_job,
    ):
        """Should update max concurrent runs."""
        # Create a test job with max_concurrent_runs=1
        tasks = [
            {
                "task_key": "test_task",
                "notebook_task": {
                    "notebook_path": test_notebook_path,
                    "source": "WORKSPACE",
                },
            }
        ]
        job = create_job(
            name="test_update_concurrent",
            tasks=tasks,
            max_concurrent_runs=1,
        )
        cleanup_job(job["job_id"])

        logger.info(f"Updating max_concurrent_runs for job {job['job_id']}")

        # Update using our core function
        update_job(job_id=job["job_id"], max_concurrent_runs=5)

        # Verify update
        updated_job = get_job(job_id=job["job_id"])
        max_runs = updated_job["settings"]["max_concurrent_runs"]
        logger.info(f"Max concurrent runs updated to: {max_runs}")

        assert max_runs == 5, "Max concurrent runs should be updated"


@pytest.mark.integration
class TestDeleteJob:
    """Tests for deleting jobs."""

    def test_delete_job(
        self,
        test_notebook_path: str,
    ):
        """Should delete a job successfully."""
        # Create a test job
        tasks = [
            {
                "task_key": "test_task",
                "notebook_task": {
                    "notebook_path": test_notebook_path,
                    "source": "WORKSPACE",
                },
            }
        ]
        job = create_job(name="test_delete_job", tasks=tasks)
        job_id = job["job_id"]

        logger.info(f"Deleting job: {job_id}")

        # Delete the job using our core function
        delete_job(job_id=job_id)

        logger.info(f"Job {job_id} deleted")

        # Verify deletion - try to get the job (should fail)
        with pytest.raises(JobError):
            get_job(job_id=job_id)

    def test_delete_job_not_found(self):
        """Should raise JobError when deleting non-existent job."""
        non_existent_id = 999999999

        logger.info(f"Deleting non-existent job: {non_existent_id}")

        with pytest.raises(JobError) as exc_info:
            delete_job(job_id=non_existent_id)

        logger.info(f"Expected JobError: {exc_info.value}")
        assert exc_info.value.job_id == non_existent_id
