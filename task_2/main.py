import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final, Any

from aiohttp import ClientSession
from config import settings
from loguru import logger

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"


@dataclass
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]


class RateLimiter:
    """Вспомогательный класс для ограничения RPS."""

    def __init__(self, rps: int):
        self.delay = 1.0 / rps
        self._last_request_time = 0.0

    async def wait(self):
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self.delay:
            wait_time = self.delay - elapsed
            await asyncio.sleep(wait_time)
        self._last_request_time = time.monotonic()


class GithubReposScrapper:
    def __init__(self, access_token: str, max_concurrent: int = 5, max_rps: int = 10):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            }
        )
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._rate_limiter = RateLimiter(max_rps)

    async def _make_request(
        self, endpoint: str, method: str = "GET", params: dict[str, Any] | None = None
    ) -> Any:
        """
        Выполняет запрос с учетом ограничений RPS и MCR.
        """
        async with self._semaphore:
            await self._rate_limiter.wait()

            try:
                url = f"{GITHUB_API_BASE_URL}/{endpoint}"
                async with self._session.request(
                    method, url, params=params
                ) as response:
                    response.raise_for_status()
                    return await response.json()
            except Exception as e:
                logger.error(f"Error fetching {endpoint}: {e}")
                return None

    async def _get_top_repositories(self, limit: int = 100) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories"""
        data = await self._make_request(
            endpoint="search/repositories",
            params={
                "q": "stars:>1",
                "sort": "stars",
                "order": "desc",
                "per_page": limit,
            },
        )
        return data.get("items", []) if data else []

    async def _get_repository_commits(
        self, owner: str, repo: str
    ) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits"""
        today_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        data = await self._make_request(
            endpoint=f"repos/{owner}/{repo}/commits",
            params={"since": today_start.isoformat(), "per_page": 100},
        )
        return data

    async def _process_repository(
        self, repo_data: dict[str, Any], position: int
    ) -> Repository | None:
        """
        Обрабатывает один репозиторий: получает коммиты и формирует DTO.
        """
        try:
            owner = repo_data["owner"]["login"]
            name = repo_data["name"]

            commits = await self._get_repository_commits(owner, name)

            authors_stats: dict[str, int] = {}
            for commit in commits:
                author_login = (
                    commit.get("author", {}).get("login")
                    if commit.get("author")
                    else commit.get("commit", {})
                    .get("author", {})
                    .get("name", "Unknown")
                )

                if author_login:
                    authors_stats[author_login] = authors_stats.get(author_login, 0) + 1

            authors_list = [
                RepositoryAuthorCommitsNum(author=k, commits_num=v)
                for k, v in authors_stats.items()
            ]

            return Repository(
                name=name,
                owner=owner,
                position=position,
                stars=repo_data.get("stargazers_count", 0),
                watchers=repo_data.get("watchers_count", 0),
                forks=repo_data.get("forks_count", 0),
                language=repo_data.get("language") or "Unknown",
                authors_commits_num_today=authors_list,
            )
        except Exception as e:
            logger.error(f"Error processing repo {repo_data.get('name')}: {e}")
            return None

    async def get_repositories(self) -> list[Repository]:
        repos_data = await self._get_top_repositories(limit=100)

        tasks = []
        for idx, repo_item in enumerate(repos_data, start=1):
            tasks.append(self._process_repository(repo_item, idx))

        results = await asyncio.gather(*tasks)

        return [r for r in results if r is not None]

    async def close(self):
        await self._session.close()


# --------------------------------


async def main():
    scraper = GithubReposScrapper(
        access_token=settings.GITHUB_TOKEN,
        max_concurrent=settings.MAX_CONCURRENT_REQUESTS,
        max_rps=settings.MAX_RPS,
    )
    try:
        repos = await scraper.get_repositories()
        for repo in repos[:5]:
            print(
                f"Repo: {repo.name}, Authors today: {len(repo.authors_commits_num_today)}"
            )
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
