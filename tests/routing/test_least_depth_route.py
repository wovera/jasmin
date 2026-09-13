from twisted.trial.unittest import TestCase
from jasmin.routing.Routes import RandomRoundrobinMTRoute
from jasmin.routing.jasminApi import SmppClientConnector
from jasmin.routing.Filters import TransparentFilter


class LeastDepthSelectionTestCase(TestCase):
    def test_picks_the_bound_connector_with_the_smallest_backlog(self):
        route = RandomRoundrobinMTRoute([TransparentFilter()],
                                        [SmppClientConnector('a'), SmppClientConnector('b'),
                                         SmppClientConnector('c')], 0.0)
        depths = {'a': 40, 'b': 3, 'c': 10}
        chosen = route.getBoundConnector(lambda cid: cid != 'c', lambda cid: depths[cid])
        self.assertEqual(chosen.cid, 'b')

    def test_no_bound_connector_answers_none(self):
        route = RandomRoundrobinMTRoute([TransparentFilter()], [SmppClientConnector('a')], 0.0)
        self.assertIsNone(route.getBoundConnector(lambda cid: False, lambda cid: 0))
