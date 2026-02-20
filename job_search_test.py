from functions import _search_jobs_tool

search_results = _search_jobs_tool("Electrician", user_id=23, limit=10, use_profile=True)
print(search_results)