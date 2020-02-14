from ..utils import flatten

from datalad.tests.utils import assert_equal

def test_flatten():

    assert_equal(flatten([]), [])
    assert_equal(flatten([1]), [1])
    assert_equal(flatten([1, 2]), [1, 2])
    # tuples etc aren't flattened
    assert_equal(flatten([1, {2}, (3, 4)]), [1, {2}, (3, 4)])
    # but we could make them flatten too!
    assert_equal(flatten([1, {2}, (3, 4)], types=(set, tuple, list)),
                 [1, 2, 3, 4])

    # recurse now
    assert_equal(flatten([1, [2, [3, 4], 5], 6]), [1, 2, 3, 4, 5, 6])

    # And now try "fancy" (original implementation target was list) types
    assert_equal(flatten(((0,), (1, 2))), (0, 1, 2))
    assert_equal(flatten(({0}, (1, 2)), types=(set, tuple)), (0, 1, 2))
    assert_equal(flatten([(0,), {1: 2}], types=(list, tuple, dict), base_type=tuple),
                 (0, 1))
