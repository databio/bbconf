from pydantic import BaseModel, ConfigDict, Field


class StatsReturn(BaseModel):
    number_of_bedfiles: int = 0
    number_of_bedsets: int = 0
    number_of_genomes: int = 0
