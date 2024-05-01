import zarr
import s3fs
import genimtools

from genimtools import __version__
import botocore

print(__version__)

from genimtools.tokenizers import TreeTokenizer


from geniml.io import RegionSet

# from genimtools.tokenizers import RegionSet


data = [1, 2, 4, 6, 7, 9, 3214]


def zarr():

    root = zarr.group(
        store="/home/bnt4me/virginia/repos/bbconf/zarr_test", overwrite=False
    )
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

    rs = RegionSet("/home/bnt4me/Downloads/0dcdf8986a72a3d85805bbc9493a1302.bed.gz")
    tokenizer = TreeTokenizer(
        "/home/bnt4me/Downloads/7126993b14054a32de2da4a0b9173be5.bed.gz"
    )

    tokens = tokenizer(rs)

    tok_regions = tokens.ids  # [42, 101, 999]
    b = tokens.to_regions()  # [Region(chr1, 100, 200), ... ]
    f = tokens.to_bit_vector()  #

    tokenized_name = "0dcdf8986a72a3d85805bbc9493a13026l"

    overwrite = True

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

    s3fc_obj = s3fs.S3FileSystem(
        endpoint_url=...,
        key=...,
        secret=...,
    )
    s3_path = f"s3://bedbase/new/"

    zarr_store = s3fs.S3Map(root=s3_path, s3=s3fc_obj, check=False, create=True)
    cache = zarr.LRUStoreCache(zarr_store, max_size=2**28)

    root = zarr.group(store=cache, overwrite=False)
    univers_group = root.require_group("7126993b14054a32de2da4a0b9173be5")


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


def bio_cache():
    pass


if __name__ == "__main__":
    # zarr()
    biocframe()
