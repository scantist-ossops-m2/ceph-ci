// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoObjectDispatch.h"
#include "librbd/crypto/Utils.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"
#include "test/librbd/mock/crypto/MockEncryptionFormat.h"
#include "test/librbd/mock/io/MockObjectDispatch.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }

  MockTestImageCtx *parent = nullptr;
};

} // anonymous namespace

namespace crypto {

template <>
struct CryptoObjectDispatch<MockTestImageCtx> : public io::MockObjectDispatch {

  static CryptoObjectDispatch* create(
          MockTestImageCtx* image_ctx, CryptoInterface* crypto) {
    return new CryptoObjectDispatch();
  }

  CryptoObjectDispatch() {
  }
};

struct MockTestEncryptionFormat : EncryptionFormat<MockTestImageCtx> {
  MockTestEncryptionFormat(std::string id) : id(id) {
  }

  std::unique_ptr<EncryptionFormat<MockTestImageCtx>> clone() const override {
    return std::unique_ptr<EncryptionFormat<MockTestImageCtx>>(clone_ptr);
  }

  MOCK_METHOD2(format, void(MockTestImageCtx*, Context*));
  MOCK_METHOD2(load, void(MockTestImageCtx*, Context*));
  MOCK_METHOD2(flatten, void(MockTestImageCtx*, Context*));
  MOCK_METHOD0(get_crypto, MockCryptoInterface*());

  std::string id;
  MockTestEncryptionFormat* clone_ptr;
};

namespace util {

template <>
void set_crypto(
        MockTestImageCtx* image_ctx,
        std::unique_ptr<EncryptionFormat<MockTestImageCtx>> format) {
  image_ctx->encryption_format.reset(new MockEncryptionFormat());
  image_ctx->encryption_format->id =
          static_cast<MockTestEncryptionFormat*>(format.get())->id;
}

} // namespace util

} // namespace crypto
} // namespace librbd

#include "librbd/crypto/LoadRequest.cc"

namespace librbd {
namespace crypto {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArgs;

struct TestMockCryptoLoadRequest : public TestMockFixture {
  typedef LoadRequest<librbd::MockTestImageCtx> MockLoadRequest;

  MockTestImageCtx* mock_image_ctx;
  MockTestImageCtx* mock_parent_image_ctx;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  MockTestEncryptionFormat* mock_encryption_format;
  Context* load_context;
  MockLoadRequest* mock_load_request;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockTestImageCtx(*ictx);
    mock_parent_image_ctx = new MockTestImageCtx(*ictx);
    mock_image_ctx->parent = mock_parent_image_ctx;
    mock_encryption_format = new MockTestEncryptionFormat("");
    mock_load_request = MockLoadRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockTestEncryptionFormat>(mock_encryption_format),
          on_finish);
  }

  void TearDown() override {
    delete mock_image_ctx;
    delete mock_parent_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_test_journal_feature(MockTestImageCtx* ictx,
                                   bool has_journal = false) {
    EXPECT_CALL(*ictx, test_features(
            RBD_FEATURE_JOURNALING)).WillOnce(Return(has_journal));
  }

  void expect_encryption_load(MockTestEncryptionFormat* encryption_format,
                              MockTestImageCtx* ictx) {
    EXPECT_CALL(*encryption_format, load(
            ictx, _)).WillOnce(
                    WithArgs<1>(Invoke([this](Context* ctx) {
                      load_context = ctx;
    })));
  }
};

TEST_F(TestMockCryptoLoadRequest, CryptoAlreadyLoaded) {
  mock_image_ctx->encryption_format.reset(new MockEncryptionFormat());
  mock_load_request->send();
  ASSERT_EQ(-EEXIST, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, JournalEnabled) {
  expect_test_journal_feature(mock_image_ctx, true);
  mock_load_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, JournalEnabledOnParent) {
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx, true);
  mock_load_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, LoadFail) {
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx);
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(-EINVAL);
  ASSERT_EQ(-EINVAL, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, Success) {
  delete mock_load_request;
  mock_image_ctx->parent = nullptr;
  mock_encryption_format = new MockTestEncryptionFormat("");
  mock_load_request = MockLoadRequest::create(
        mock_image_ctx,
        std::unique_ptr<MockTestEncryptionFormat>(mock_encryption_format),
        on_finish);
  expect_test_journal_feature(mock_image_ctx);
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(nullptr, mock_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLoadRequest, LoadClonedEncryptedParent) {
  mock_encryption_format->clone_ptr = new MockTestEncryptionFormat("clone");
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx);
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_load(
          mock_encryption_format->clone_ptr, mock_parent_image_ctx);
  load_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ("", mock_image_ctx->encryption_format.get()->id);
  ASSERT_EQ("clone",
            mock_parent_image_ctx->encryption_format.get()->id);
}

TEST_F(TestMockCryptoLoadRequest, LoadClonedParentFail) {
  mock_encryption_format->clone_ptr = new MockTestEncryptionFormat("clone");
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx);
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_load(
          mock_encryption_format->clone_ptr, mock_parent_image_ctx);
  load_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->encryption_format.get());
  ASSERT_EQ(nullptr, mock_parent_image_ctx->encryption_format.get());
}

} // namespace crypto
} // namespace librbd
