import asyncio
from datetime import date, datetime

from aiochclient import ChClient
from aiohttp import ClientSession

from task_2.main import GithubReposScrapper, Repository
from task_3.config import settings
from loguru import logger


class GithubToClickHouseLoader:
    def __init__(self, scraper: GithubReposScrapper, ch_client: ChClient):
        self.scraper = scraper
        self.ch_client = ch_client

    async def save_repositories(self, repositories: list[Repository]):
        if not repositories:
            return

        today = date.today()
        now = datetime.now().replace(microsecond=0)

        repos_rows = []

        positions_rows = []

        commits_rows = []

        for repo in repositories:
            repos_rows.append(
                (
                    repo.name,
                    repo.owner,
                    repo.stars,
                    repo.watchers,
                    repo.forks,
                    repo.language,
                    now,
                )
            )

            positions_rows.append((today, repo.name, repo.position))

            for author_stat in repo.authors_commits_num_today:
                commits_rows.append(
                    (today, repo.name, author_stat.author, author_stat.commits_num)
                )

        try:
            if repos_rows:
                await self.ch_client.execute(
                    f"INSERT INTO {settings.CLICKHOUSE_DB}.repositories "
                    "(name, owner, stars, watchers, forks, language, updated) VALUES",
                    *repos_rows,
                )

            if positions_rows:
                await self.ch_client.execute(
                    f"INSERT INTO {settings.CLICKHOUSE_DB}.repositories_positions "
                    "(date, repo, position) VALUES",
                    *positions_rows,
                )

            if commits_rows:
                await self.ch_client.execute(
                    f"INSERT INTO {settings.CLICKHOUSE_DB}.repositories_authors_commits "
                    "(date, repo, author, commits_num) VALUES",
                    *commits_rows,
                )

            logger.success(f"Successfully inserted {len(repositories)} repositories.")

        except Exception as e:
            logger.error(f"Failed to insert data into ClickHouse: {e}")
            raise


async def main():
    session = ClientSession()

    scraper = GithubReposScrapper(access_token=settings.GITHUB_TOKEN)

    ch_client = ChClient(
        session=session,
        url=settings.CLICKHOUSE_URL,
        user=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
    )

    loader = GithubToClickHouseLoader(scraper, ch_client)

    try:
        logger.info("Fetching repositories...")
        repos = await scraper.get_repositories()
        logger.info(f"Fetched {len(repos)} repos. Saving to ClickHouse...")
        await loader.save_repositories(repos)
    finally:
        await scraper.close()
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
