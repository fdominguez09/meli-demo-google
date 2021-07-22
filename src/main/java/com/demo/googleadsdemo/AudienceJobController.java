package com.demo.googleadsdemo;

import com.google.ads.googleads.lib.GoogleAdsClient;
import com.google.ads.googleads.v8.common.CrmBasedUserListInfo;
import com.google.ads.googleads.v8.common.CustomerMatchUserListMetadata;
import com.google.ads.googleads.v8.common.UserData;
import com.google.ads.googleads.v8.common.UserIdentifier;
import com.google.ads.googleads.v8.enums.CustomerMatchUploadKeyTypeEnum;
import com.google.ads.googleads.v8.enums.OfflineUserDataJobStatusEnum;
import com.google.ads.googleads.v8.enums.OfflineUserDataJobTypeEnum;
import com.google.ads.googleads.v8.errors.GoogleAdsError;
import com.google.ads.googleads.v8.errors.GoogleAdsException;
import com.google.ads.googleads.v8.resources.OfflineUserDataJob;
import com.google.ads.googleads.v8.resources.UserList;
import com.google.ads.googleads.v8.services.*;
import com.google.api.gax.rpc.ServerStream;
import com.google.common.collect.ImmutableList;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@RestController
public class AudienceJobController {
    @GetMapping("/demo")
    public String demo() throws Exception{
        GoogleAdsClient googleAdsClient = null;
        try (InputStream input = AudienceJobController.class.getClassLoader().getResourceAsStream("ads.properties")) {
            Properties prop = new Properties();

            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return "Missing ads.properties file";
            }

            //load a properties file from class path
            prop.load(input);

            googleAdsClient = GoogleAdsClient.newBuilder().fromProperties(prop).build();
        } catch (FileNotFoundException fnfe) {
            System.err.printf(
                    "Failed to load GoogleAdsClient configuration from file. Exception: %s%n", fnfe);
            throw fnfe;
        } catch (IOException ioe) {
            System.err.printf("Failed to create GoogleAdsClient. Exception: %s%n", ioe);
            throw ioe;
        }

        try {
            runExample(googleAdsClient, 1164191532);
        } catch (GoogleAdsException gae) {
            // GoogleAdsException is the base class for most exceptions thrown by an API request.
            // Instances of this exception have a message and a GoogleAdsFailure that contains a
            // collection of GoogleAdsErrors that indicate the underlying causes of the
            // GoogleAdsException.
            System.err.printf(
                    "Request ID %s failed due to GoogleAdsException. Underlying errors:%n",
                    gae.getRequestId());
            int i = 0;
            for (GoogleAdsError googleAdsError : gae.getGoogleAdsFailure().getErrorsList()) {
                System.err.printf("  Error %d: %s%n", i++, googleAdsError);
            }

            throw gae;
        } catch (UnsupportedEncodingException gae) {
            gae.printStackTrace();
            throw gae;
        } catch (Exception gae) {
            gae.printStackTrace();
            throw gae;
        }
        return "ok";
    }

    /**
     * Runs the example.
     *
     * @param customerId the client customer ID.
     * @throws GoogleAdsException if an API request failed with one or more service errors.
     */
    public void runExample(GoogleAdsClient googleAdsClient, long customerId)
            throws Exception {
        // Creates a Customer Match user list.
        String userListResourceName = createCustomerMatchUserList(googleAdsClient, customerId);
        System.out.println("userListResourceName "+userListResourceName);

        // Adds members to the user list.
        String offlineUserDataJobResourceName = addUsersToCustomerMatchUserList(googleAdsClient, customerId, userListResourceName);
        System.out.println("offlineUserDataJobResourceName "+offlineUserDataJobResourceName);

    }

    /**
     * Creates a Customer Match user list.
     *
     * @param customerId the client customer ID.
     * @return the resource name of the newly created user list.
     */
    private String createCustomerMatchUserList(GoogleAdsClient googleAdsClient, long customerId) {
        // Creates the new user list.
        UserList userList =
                UserList.newBuilder()
                        .setName("Customer Match list #" + getPrintableDateTime())
                        .setDescription("A list of customers that originated from email addresses")
                        // Customer Match user lists can use a membership life span of 10,000 to indicate
                        // unlimited; otherwise normal values apply.
                        // Sets the membership life span to 30 days.
                        .setMembershipLifeSpan(30)
                        // Sets the upload key type to indicate the type of identifier that will be used to
                        // add users to the list. This field is immutable and required for an ADD operation.
                        .setCrmBasedUserList(
                                CrmBasedUserListInfo.newBuilder()
                                        .setUploadKeyType(CustomerMatchUploadKeyTypeEnum.CustomerMatchUploadKeyType.CONTACT_INFO))
                        //.setUploadKeyType(CustomerMatchUploadKeyType.MOBILE_ADVERTISING_ID).setAppId("com.mercadolibre"))
                        .build();

        // Creates the operation.
        UserListOperation operation = UserListOperation.newBuilder().setCreate(userList).build();

        // Creates the service client.
        try (UserListServiceClient userListServiceClient =
                     googleAdsClient.getLatestVersion().createUserListServiceClient()) {

            // Adds the user list.
            MutateUserListsResponse response =
                    userListServiceClient.mutateUserLists(
                            Long.toString(customerId), ImmutableList.of(operation));
            // Prints the response.
            System.out.printf(
                    "Created Customer Match user list with resource name: %s.%n",
                    response.getResults(0).getResourceName());
            return response.getResults(0).getResourceName();
        }
    }

    /**
     * Creates and executes an asynchronous job to add users to the Customer Match user list.
     * @param googleAdsClient
     * @param customerId           the client customer ID.
     * @param userListResourceName the resource name of the Customer Match user list to add members.
     *                             to.
     */
    private String addUsersToCustomerMatchUserList(GoogleAdsClient googleAdsClient,
                                                   long customerId, String userListResourceName)
            throws UnsupportedEncodingException {
        String offlineUserDataJobResourceName = null;
        try (OfflineUserDataJobServiceClient offlineUserDataJobServiceClient =
                     googleAdsClient.getLatestVersion().createOfflineUserDataJobServiceClient()) {
            // Creates a new offline user data job.
            OfflineUserDataJob offlineUserDataJob =
                    OfflineUserDataJob.newBuilder()
                            .setType(OfflineUserDataJobTypeEnum.OfflineUserDataJobType.CUSTOMER_MATCH_USER_LIST)
                            .setCustomerMatchUserListMetadata(
                                    CustomerMatchUserListMetadata.newBuilder().setUserList(userListResourceName))
                            .build();

            // Issues a request to create the offline user data job.
            CreateOfflineUserDataJobResponse createOfflineUserDataJobResponse =
                    offlineUserDataJobServiceClient.createOfflineUserDataJob(
                            Long.toString(customerId), offlineUserDataJob);
            offlineUserDataJobResourceName = createOfflineUserDataJobResponse.getResourceName();
            System.out.printf(
                    "Created an offline user data job with resource name: %s.%n",
                    offlineUserDataJobResourceName);

            // Issues a request to add the operations to the offline user data job.
            List<OfflineUserDataJobOperation> userDataJobOperations = buildOfflineUserDataJobOperations();
            AddOfflineUserDataJobOperationsResponse response =
                    offlineUserDataJobServiceClient.addOfflineUserDataJobOperations(
                            AddOfflineUserDataJobOperationsRequest.newBuilder()
                                    .setResourceName(offlineUserDataJobResourceName)
                                    .setEnablePartialFailure(true)
                                    .addAllOperations(userDataJobOperations)
                                    .build());

            // Prints the status message if any partial failure error is returned.
            // NOTE: The details of each partial failure error are not printed here, you can refer to
            // the example HandlePartialFailure.java to learn more.
            if (response.hasPartialFailureError()) {
                System.out.printf(
                        "Encountered %d partial failure errors while adding %d operations to the offline user "
                                + "data job: '%s'. Only the successfully added operations will be executed when "
                                + "the job runs.%n",
                        response.getPartialFailureError().getDetailsCount(),
                        userDataJobOperations.size(),
                        response.getPartialFailureError().getMessage());
            } else {
                System.out.printf(
                        "Successfully added %d operations to the offline user data job.%n",
                        userDataJobOperations.size());
            }


            // Issues an asynchronous request to run the offline user data job for executing
            // all added operations.
        }
        try (OfflineUserDataJobServiceClient offlineUserDataJobServiceClient =
                     googleAdsClient.getLatestVersion().createOfflineUserDataJobServiceClient()) {
            offlineUserDataJobServiceClient.runOfflineUserDataJobAsync(offlineUserDataJobResourceName);
            /*
            checkJobStatus(googleAdsClient, customerId, offlineUserDataJobResourceName, userListResourceName);
             */
        }

        return offlineUserDataJobResourceName;

    }

    /**
     * Creates a list of offline user data job operations that will add users to the list.
     *
     * @return a list of operations.
     */
    private List<OfflineUserDataJobOperation> buildOfflineUserDataJobOperations()
            throws UnsupportedEncodingException {
        MessageDigest sha256Digest;
        try {
            sha256Digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Missing SHA-256 algorithm implementation", e);
        }

        UserData userDataWithEmailAddress1 = null;
        List<UserData> listaUsers = new ArrayList<>();

        int i = 0;
        while (i < 1000) {
            userDataWithEmailAddress1 = UserData.newBuilder()
                    .addUserIdentifiers(
                            UserIdentifier.newBuilder()
                                    .setHashedEmail(normalizeAndHash(sha256Digest, "customer" + i + "@example.com")))
                    .build();
            listaUsers.add(userDataWithEmailAddress1);
            i++;
        }

        // Creates the operations to add the users.
        List<OfflineUserDataJobOperation> operations = new ArrayList<>();

        for (UserData userData : listaUsers) {
            operations.add(OfflineUserDataJobOperation.newBuilder().setCreate(userData).build());
        }

        return operations;
    }

    /**
     * Returns the result of normalizing and then hashing the string using the provided digest.
     * Private customer data must be hashed during upload, as described at
     * https://support.google.com/google-ads/answer/7474263.
     *
     * @param digest the digest to use to hash the normalized string.
     * @param s      the string to normalize and hash.
     */
    private String normalizeAndHash(MessageDigest digest, String s)
            throws UnsupportedEncodingException {
        // Normalizes by removing leading and trailing whitespace and converting all characters to
        // lower case.
        String normalized = s.trim().toLowerCase();
        // Hashes the normalized string using the hashing algorithm.
        byte[] hash = digest.digest(normalized.getBytes("UTF-8"));
        StringBuilder result = new StringBuilder();
        for (byte b : hash) {
            result.append(String.format("%02x", b));
        }

        return result.toString();
    }

    /**
     * Retrieves, checks, and prints the status of the offline user data job.
     *
     * @param googleAdsClient                the Google Ads API client.
     * @param customerId                     the client customer ID.
     * @param offlineUserDataJobResourceName the resource name of the OfflineUserDataJob to get the
     *                                       status for.
     * @param userListResourceName           the resource name of the Customer Match user list.
     */
    private void checkJobStatus(
            GoogleAdsClient googleAdsClient,
            long customerId,
            String offlineUserDataJobResourceName,
            String userListResourceName) {
        try (GoogleAdsServiceClient googleAdsServiceClient =
                     googleAdsClient.getLatestVersion().createGoogleAdsServiceClient()) {
            String query =
                    String.format(
                            "SELECT offline_user_data_job.resource_name, "
                                    + "offline_user_data_job.id, "
                                    + "offline_user_data_job.status, "
                                    + "offline_user_data_job.type, "
                                    + "offline_user_data_job.failure_reason "
                                    + "FROM offline_user_data_job "
                                    + "WHERE offline_user_data_job.resource_name = '%s'",
                            offlineUserDataJobResourceName);
            // Issues the query and gets the GoogleAdsRow containing the job from the response.
            GoogleAdsRow googleAdsRow =
                    googleAdsServiceClient
                            .search(Long.toString(customerId), query)
                            .iterateAll()
                            .iterator()
                            .next();
            OfflineUserDataJob offlineUserDataJob = googleAdsRow.getOfflineUserDataJob();
            System.out.printf(
                    "Offline user data job ID %d with type '%s' has status: %s%n",
                    offlineUserDataJob.getId(), offlineUserDataJob.getType(), offlineUserDataJob.getStatus());
            OfflineUserDataJobStatusEnum.OfflineUserDataJobStatus jobStatus = offlineUserDataJob.getStatus();
            if (OfflineUserDataJobStatusEnum.OfflineUserDataJobStatus.SUCCESS == jobStatus) {
                // Prints information about the user list.
                printCustomerMatchUserListInfo(googleAdsClient, customerId, userListResourceName);
            } else if (OfflineUserDataJobStatusEnum.OfflineUserDataJobStatus.FAILED == jobStatus) {
                System.out.printf("  Failure reason: %s%n", offlineUserDataJob.getFailureReason());
            } else if (OfflineUserDataJobStatusEnum.OfflineUserDataJobStatus.PENDING == jobStatus
                    || OfflineUserDataJobStatusEnum.OfflineUserDataJobStatus.RUNNING == jobStatus) {
                System.out.println();
                System.out.printf(
                        "To check the status of the job periodically, use the following GAQL query with"
                                + " GoogleAdsService.search:%n%s%n",
                        query);
            }
        }
    }

    /**
     * Prints information about the Customer Match user list.
     *
     * @param googleAdsClient      the Google Ads API client.
     * @param customerId           the client customer ID .
     * @param userListResourceName the resource name of the Customer Match user list.
     */
    private void printCustomerMatchUserListInfo(
            GoogleAdsClient googleAdsClient, long customerId, String userListResourceName) {
        try (GoogleAdsServiceClient googleAdsServiceClient =
                     googleAdsClient.getLatestVersion().createGoogleAdsServiceClient()) {
            // Creates a query that retrieves the user list.
            String query =
                    String.format(
                            "SELECT user_list.size_for_display, user_list.size_for_search "
                                    + "FROM user_list "
                                    + "WHERE user_list.resource_name = '%s'",
                            userListResourceName);

            // Constructs the SearchGoogleAdsStreamRequest.
            SearchGoogleAdsStreamRequest request =
                    SearchGoogleAdsStreamRequest.newBuilder()
                            .setCustomerId(Long.toString(customerId))
                            .setQuery(query)
                            .build();

            // Issues the search stream request.
            ServerStream<SearchGoogleAdsStreamResponse> stream =
                    googleAdsServiceClient.searchStreamCallable().call(request);

            // Gets the first and only row from the response.
            GoogleAdsRow googleAdsRow = stream.iterator().next().getResultsList().get(0);
            UserList userList = googleAdsRow.getUserList();
            System.out.printf(
                    "User list '%s' has an estimated %d users for Display and %d users for Search.%n",
                    userList.getResourceName(), userList.getSizeForDisplay(), userList.getSizeForSearch());
            System.out.println(
                    "Reminder: It may take several hours for the user list to be populated with the users.");
        }
    }

    private static final DateTimeFormatter format =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

    /**
     * The shorter date format used for printing.
     */
    private static final DateTimeFormatter shortFormat =
            DateTimeFormatter.ofPattern("MMddHHmmssSSS");

    public static String getPrintableDateTime() {
        return ZonedDateTime.now().format(format);
    }
}



