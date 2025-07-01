import os

import s3fs
import zarr

# from dotenv import load_dotenv
from geniml.io import RegionSet
from gtars.utils import read_tokens_from_gtok
import matplotlib.pyplot as plt
import numpy as np
import time

# from gtars.tokenizers import RegionSet


# load_dotenv()

data = [1, 2, 4, 6, 7, 9, 3214]


def create_tokenized():
    rs = RegionSet("/home/bnt4me/Downloads/0dcdf8986a72a3d85805bbc9493a1302.bed.gz")
    tokenizer = TreeTokenizer(
        "/home/bnt4me/Downloads/7126993b14054a32de2da4a0b9173be5.bed.gz"
    )

    tokens = tokenizer(rs)

    tok_regions = tokens.ids  # [42, 101, 999]
    b = tokens.to_regions()  # [Region(chr1, 100, 200), ... ]
    f = tokens.to_bit_vector()  #

    return tok_regions


def zarr_local():
    tok_regions = create_tokenized()
    tokenized_name = "0dcdf8986a72a3d85805bbc9493a13026l"
    overwrite = True

    root = zarr.group(
        store="/home/bnt4me/virginia/repos/bbconf/zarr_test", overwrite=False
    )

    univers_group = root.require_group("7126993b14054a32de2da4a0b9173be5")
    if not univers_group.get(tokenized_name):
        print("not overwriting")
        ua = univers_group.create_dataset(tokenized_name, data=tok_regions)
    elif overwrite:
        print("overwriting")
        ua = univers_group.create_dataset(
            tokenized_name, data=tok_regions, overwrite=True
        )
    else:
        raise ValueError("fff")
    ua = univers_group
    univers_group._delitem_nosync()


def zarr_s3():

    # foo = root.create_group('foo')
    # bar = foo.create_group('bar')
    #
    # # bar.zeros('baz', shape=(10000, 10000), chunks=(1000, 1000), dtype='i4')
    # bar.create_dataset('nested_dataset1', data=data)

    # f = zarr.open_group('/home/bnt4me/virginia/repos/bbconf/zarr_test/', mode='r',)
    #
    # print(sorted(f.groups())[0])
    # print(f.tree())
    # print(f["foo"].tree())
    # print(f.foo.bar.tree())
    #
    # print(f.foo.bar.nested_dataset1[:])
    # print(f.foo.bar.nested_dataset1)
    #
    # print(list(f.get('foo/bar/nested_dataset1')[:]))
    #
    # print([k for k in f.group_keys()])

    tok_regions = create_tokenized()
    tokenized_name = "0dcdf8986a72a3d85805bbc9493a13026l"
    overwrite = True

    s3fc_obj = s3fs.S3FileSystem(
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    print(os.getenv("AWS_SECRET_ACCESS_KEY"))
    s3_path = "s3://bedbase/new/"

    zarr_store = s3fs.S3Map(root=s3_path, s3=s3fc_obj, check=False, create=True)
    cache = zarr.LRUStoreCache(zarr_store, max_size=2**28)

    root = zarr.group(store=cache, overwrite=False)
    univers_group = root.require_group("7126993b14054a32de2da4a0b9173be5")
    univers_group.create_dataset(tokenized_name, data=tok_regions, overwrite=True)

    f = univers_group[tokenized_name]

    print(f)


def get_from_s3():
    s3fc_obj = s3fs.S3FileSystem(
        endpoint_url="https://data2.bedbase.org/",
        # endpoint_url="https://s3.us-west-002.backblazeb2.com/",
        # key=os.getenv("AWS_ACCESS_KEY_ID"),
        # secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    import s3fs

    s3fc_obj = s3fs.S3FileSystem(endpoint_url="https://s3.us-west-002.backblazeb2.com/")
    s3_path = "s3://bedbase/tokenized.zarr/"
    zarr_store = s3fs.S3Map(root=s3_path, s3=s3fc_obj, check=False, create=True)
    cache = zarr.LRUStoreCache(zarr_store, max_size=2**28)

    root = zarr.group(store=cache, overwrite=False)
    # print(str(root.tree))


def biocframe():
    from biocframe import BiocFrame

    obj = {
        "column1": [1, 2, 3],
        "nested": [
            {
                "ncol1": [4, 5, 6],
                "ncol2": ["a", "b", "c"],
                "deep": {"dcol1": ["j", "k", "l"], "dcol2": ["a", "s", "l"]},
            },
            {
                "ncol2": ["a"],
                "deep": {"dcol1": ["j"], "dcol2": ["a"]},
            },
            {
                "ncol1": [5, 6],
                "ncol2": ["b", "c"],
            },
        ],
        "column2": ["b", "n", "m"],
    }

    bframe = BiocFrame(obj)

    ff = bframe.to_pandas()

    ff


def add_s3():
    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")

    agent.bed.add_universe(bedfile_id="dbf25622412f0092798293f0b3b2050d")

    agent.bed.add_tokenized(
        universe_id="dbf25622412f0092798293f0b3b2050d",
        bed_id="ab3641b731386f8f7d865ab82cdb4c83",
        token_vector=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    )


def get_pep():
    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")

    f = agent.bedset.get_bedset_pep(identifier="encode_batch_1")
    import peppy

    prj = peppy.Project.from_dict(f)
    prj


def get_id_plots_missing():
    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")

    results = agent.bed.get_missing_plots("gccontent", limit=5000)
    print(results)
    print(agent.get_list_genomes())


def neighbour_beds():
    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")
    results = agent.bed.get_neighbours("e76e41597622b3df45435dde1a8eb19d")
    results


def sql_search():
    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")
    results = agent.bed.sql_search("", limit=100, genome="mm39")
    results


def config_t():
    from bbconf.config_parser.utils import config_analyzer

    is_valid = config_analyzer("/home/bnt4me/virginia/repos/bbconf/config.yaml")

    print(is_valid)


def compreh_stats():
    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")

    time1 = time.time()

    # results = agent.get_detailed_stats()

    results = agent.get_detailed_usage()

    time2 = time.time()
    print(time2 - time1)

    # results = agent.get_detailed_stats()
    # results = agent.get_detailed_usage()

    def plot_file_sizes(file_size_counts, file_size_bin_edges):

        # Plot the histogram
        plt.figure(figsize=(10, 6))
        plt.bar(
            file_size_bin_edges[:-1],
            file_size_counts,
            width=np.diff(file_size_bin_edges),
            edgecolor="black",
            align="edge",
        )

        # Add labels and title
        plt.xlabel("File Size Bin Edges (bytes)")
        plt.ylabel("Counts")
        plt.title("Histogram of File Sizes")
        plt.grid(axis="y", linestyle="--", alpha=0.7)

        # Show the plot
        plt.show()

    def plot_region_width(mean_reg_width_counts, mean_reg_width_bin_edges):

        # Plot the histogram
        plt.figure(figsize=(10, 6))
        plt.bar(
            mean_reg_width_bin_edges[:-1],
            mean_reg_width_counts,
            width=np.diff(mean_reg_width_bin_edges),
            edgecolor="black",
            align="edge",
        )

        # Add labels and title
        plt.xlabel("Mean Region Width Bin Edges")
        plt.ylabel("Counts")
        plt.title("Histogram of Mean Region Widths")
        plt.grid(axis="y", linestyle="--", alpha=0.7)

        # Show the plot
        plt.show()

    def plot_number_of_regions(n_region_counts, n_region_bin_edges):

        # Plot the histogram
        plt.figure(figsize=(10, 6))
        plt.bar(
            n_region_bin_edges[:-1],
            n_region_counts,
            width=np.diff(n_region_bin_edges),
            edgecolor="black",
            align="edge",
        )

        # Add labels and title
        plt.xlabel("Region Bin Edges")
        plt.ylabel("Counts")
        plt.title("Histogram of Number of Regions")
        plt.grid(axis="y", linestyle="--", alpha=0.7)

        # Show the plot
        plt.show()

    plot_file_sizes(
        results.file_size.counts,
        results.file_size.bins,
    )
    plot_region_width(
        results.mean_region_width.counts,
        results.mean_region_width.bins,
    )
    plot_number_of_regions(
        results.number_of_regions.counts,
        results.number_of_regions.bins,
    )


def get_unprocessed_files():
    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")
    results = agent.bed.get_unprocessed()
    print(results)


def get_genomes():

    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")
    results = agent.get_list_genomes()
    print(results)


def new_search():
    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")
    time1 = time.time()

    results = agent.bed.reindex_semantic_search()
    # results = agent.bed.comp_search()
    time2 = time.time()

    print(f"Time taken: {time2 - time1} seconds")


def get_assay_list():
    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")
    results = agent.get_list_assays()
    print(results)


def external_search():
    from bbconf import BedBaseAgent

    agent = BedBaseAgent(config="/home/bnt4me/virginia/repos/bedhost/config.yaml")

    result = agent.bed.search_external_file("geo", "gsm1399546")
    result


if __name__ == "__main__":
    # zarr_s3()
    # add_s3()
    # get_from_s3()
    # biocframe()
    # get_pep()
    # get_id_plots_missing()
    # neighbour_beds()
    # sql_search()
    # config_t()
    # compreh_stats()
    # get_unprocessed_files()
    # get_genomes()
    # new_search()

    external_search()
    # get_assay_list()
