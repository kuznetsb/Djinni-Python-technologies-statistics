import asyncio
import csv
import os.path
import re
from dataclasses import dataclass, fields, astuple
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup


URL = "https://djinni.co/jobs/"
SEARCH_URL = urljoin(URL, "?primary_keyword=Python")


@dataclass
class Job:
    title: str
    technologies: list[str]
    experience_years: int
    salary: int
    views: int
    applications: int


JOB_FIELDS = [field.name for field in fields(Job)]


def get_num_pages(page_soup: BeautifulSoup) -> int:
    pagination = page_soup.select_one(".pagination_with_numbers")

    if pagination is None:
        return 1

    return int(pagination.select("li")[-2].text)


async def parse_single_job(job_url: str) -> Job:
    async with aiohttp.ClientSession() as session:
        full_url = urljoin(URL, job_url)
        async with session.get(full_url, ssl=False) as resp:
            page = await resp.text()
            job_soup = BeautifulSoup(page, "html.parser")
            job_info = job_soup.select(".job-additional-info--body")[0]
            tech_spans = job_info.select(".job-additional-info--item")[1].select(
                ".job-additional-info--item-text span"
            )
            experience = (
                job_info.select(".job-additional-info--item")[-1]
                .select_one(".job-additional-info--item-text")
                .text.split()[0]
            )
            salary = job_soup.select_one(".public-salary-item")
            salary_info = re.match(r"[$]\d+", salary.text) if salary else None
            additional = (
                job_soup.select(".profile-page-section")[-1]
                .select_one("p")
                .text.split()[-4:]
            )

            return Job(
                title=job_soup.select_one(".detail--title-wrapper h1")
                .text.strip()
                .split("\n")[0],
                technologies=[span.text.lower() for span in tech_spans]
                if tech_spans
                else None,
                experience_years=int(experience) if experience.isdigit() else 0,
                salary=int(salary_info.group(0).replace("$", ""))
                if salary_info
                else None,
                views=int(additional[0]),
                applications=int(additional[2]),
            )


async def get_single_page_jobs(page_soup: BeautifulSoup) -> list[Job]:
    links = page_soup.select(".list-jobs__title a.profile")

    jobs = await asyncio.gather(*[parse_single_job(link["href"]) for link in links])
    return jobs


async def get_page_soup(url: str) -> BeautifulSoup:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, ssl=False) as resp:
            page = await resp.text()
            soup = BeautifulSoup(page, "html.parser")

            return soup


async def get_page_info(url: str) -> list[Job]:
    soup = await get_page_soup(url)
    jobs = await get_single_page_jobs(soup)
    return jobs


async def get_all_jobs() -> list[Job]:
    first_page = await get_page_soup(url=SEARCH_URL)
    pages_count = get_num_pages(first_page)

    all_jobs = await get_single_page_jobs(first_page)

    new_jobs = await asyncio.gather(
        *[
            get_page_info(SEARCH_URL + f"&page={page_num}")
            for page_num in range(2, pages_count + 1)
        ]
    )

    for job in new_jobs:
        all_jobs.extend(job)

    return all_jobs


def write_jobs_to_csv(jobs: list[Job], output_csv_path: str) -> None:
    full_path = os.path.join("data", output_csv_path)
    with open(full_path, "w") as file:
        writer = csv.writer(file)
        writer.writerow(JOB_FIELDS)
        writer.writerows([astuple(job) for job in jobs])


async def main(output_csv_path: str) -> None:
    jobs = await get_all_jobs()
    write_jobs_to_csv(jobs=jobs, output_csv_path=output_csv_path)


if __name__ == "__main__":
    asyncio.run(main("jobs.csv"))
